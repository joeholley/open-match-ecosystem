// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// package omclient is a thin client to do grpc-gateway RESTful HTTP gRPC calls
// with affordances for retry with exponential backoff + jitter.
package omclient

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	pb "github.com/googleforgames/open-match2/v2/pkg/pb"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"golang.org/x/oauth2"
	"google.golang.org/api/idtoken"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

var MMFsComplete = errors.New("Open Match HTTP client detected that the stream of matches from OM was complete and cancelled the context")

// ActivationResult represents the outcome of activating a single ticket ID in om-core.
type ActivationResult struct {
	TicketId string
	Err      error
}

type cachedIDToken struct {
	source oauth2.TokenSource
	token  *oauth2.Token
}

type idTokenCache struct {
	mint    func(ctx context.Context, audience string) (oauth2.TokenSource, error)
	mu      sync.Mutex
	entries map[string]*cachedIDToken
}

func newIDTokenCache(mint func(ctx context.Context, audience string) (oauth2.TokenSource, error)) *idTokenCache {
	return &idTokenCache{
		mint:    mint,
		entries: make(map[string]*cachedIDToken),
	}
}

type RestfulOMGrpcClient struct {
	Client *http.Client
	Log    *logrus.Logger
	Cfg    *viper.Viper
	tokens *idTokenCache
}

// New constructs a RestfulOMGrpcClient with an initialized Cloud Run ID token cache.
func New(cfg *viper.Viper, log *logrus.Logger, client *http.Client) *RestfulOMGrpcClient {
	return &RestfulOMGrpcClient{
		Client: client,
		Log:    log,
		Cfg:    cfg,
		tokens: newIDTokenCache(func(ctx context.Context, audience string) (oauth2.TokenSource, error) {
			return idtoken.NewTokenSource(ctx, audience)
		}),
	}
}

// CreateTicket Example
// Your platform services layer should take the matchmaking request, add
// attributes your platform services have authority over (ex: MMR, ELO,
// inventory, etc), then call Open Match core on the client's behalf to make a
// matchmaking ticket. In this sense, Open Match sees your platform services
// layer as a 'proxy' for the player's game client.
func (rc *RestfulOMGrpcClient) CreateTicket(ctx context.Context, ticket *pb.Ticket) (string, error) {
	// Derive from the caller's ctx (rather than context.Background()) so caller
	// deadlines and graceful shutdown cancellation propagate into rc.Post.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	logger := rc.Log.WithFields(logrus.Fields{
		"component": "open_match_client",
		"operation": "proxy_CreateTicket",
	})

	// Update metrics
	//createdTicketCounter.Add(ctx, 1)

	// Put the ticket into open match CreateTicket request protobuf message.
	reqPb := &pb.CreateTicketRequest{Ticket: ticket}
	resPb := &pb.CreateTicketResponse{}
	var err error
	var buf []byte

	// Marshal request into json
	buf, err = protojson.Marshal(reqPb)
	if err != nil {
		// Mark as a permanent error so the backoff library doesn't retry this (invalid input)
		err := backoff.Permanent(err)
		logger.WithFields(logrus.Fields{
			"pb_message": "CreateTicketsRequest",
		}).Errorf("cannot marshal proto to json")
		return "", err
	}

	// HTTP version of the gRPC CreateTicket() call
	resp, err := rc.Post(ctx, logger, rc.Cfg.GetString("OM_CORE_ADDR"), "/tickets", buf)

	// Include headers in structured logging when using trace log level [slow]
	headerFields := logrus.Fields{}
	if resp != nil && logrus.IsLevelEnabled(logrus.TraceLevel) {
		for key, value := range resp.Header {
			headerFields[key] = value
		}
	}
	logger = logger.WithFields(headerFields)

	// Check for failures
	if resp != nil && resp.StatusCode != http.StatusOK { // HTTP error code
		err := fmt.Errorf("%s (%d)", http.StatusText(resp.StatusCode), resp.StatusCode)
		logger.WithFields(logrus.Fields{
			"CreateTicketsRequest": reqPb,
		}).Errorf("CreateTicket failed: %v", err)
		//TODO: switch resp.Header?
		resp.Body.Close() // make sure the body is closed.
		return "", err
	}
	if err != nil { // HTTP library error
		// ERRO[59639] CreateTicket failed: Post "http://localhost:8080/tickets": dial tcp [::1]:8080: connect: cannot assign requested address
		logger.WithFields(logrus.Fields{
			"CreateTicketsRequest": reqPb,
		}).Errorf("CreateTicket failed: %v", err)
		// Shouldn't have a response but if somehow there is one, make sure it is closed.
		if resp != nil {
			resp.Body.Close()
		}
		return "", err
	}

	// Read & close HTTP response body
	body, err := readAllBody(*resp, logger)
	if err != nil {
		// Mark as a permanent error so the backoff library doesn't retry this REST call
		err := backoff.Permanent(err)
		logger.WithFields(logrus.Fields{
			"pb_message": "CreateTicketsResponse",
			"error":      err,
		}).Errorf("cannot read http response body")
		return "", err
	}

	// Success, unmarshal json back into protobuf
	err = protojson.Unmarshal(body, resPb)
	if err != nil {
		// Mark as a permanent error so the backoff library doesn't retry this REST call
		err := backoff.Permanent(err)
		logger.WithFields(headerFields).WithFields(logrus.Fields{
			"response_body": string(body),
			"pb_message":    "CreateTicketsResponse",
			"error":         err,
		}).Errorf("cannot unmarshal http response body back into protobuf")
		return "", err
	}
	if resPb.GetTicketId() == "" {
		// Mark as a permanent error so the backoff library doesn't retry this REST call
		err := backoff.Permanent(errors.New("CreateTicket returned empty result"))
		logger.Error(err)
		return "", err
	}

	// Successful ticket creation
	logger.Tracef("CreateTicket %v complete", resPb.TicketId)
	return resPb.TicketId, err
}

// parseTicketStatusBody unmarshals a grpc-gateway JSON google.rpc.Status body
// and extracts per-ticket field violations from any attached
// google.rpc.BadRequest details (where om-core encodes fields as "ticket_id/<id>").
// It returns ok=true when at least one per-ticket error detail was found.
func parseTicketStatusBody(body []byte) (code codes.Code, perTicketErrs map[string]string, ok bool) {
	if len(body) == 0 {
		return codes.Unknown, nil, false
	}
	var sProto spb.Status
	if err := protojson.Unmarshal(body, &sProto); err != nil {
		return codes.Unknown, nil, false
	}
	st := status.FromProto(&sProto)
	perTicketErrs = make(map[string]string)
	for _, detail := range st.Details() {
		if br, isBadReq := detail.(*errdetails.BadRequest); isBadReq {
			for _, fv := range br.GetFieldViolations() {
				ticketID := strings.TrimPrefix(fv.GetField(), "ticket_id/")
				perTicketErrs[ticketID] = fv.GetDescription()
			}
		}
	}
	return st.Code(), perTicketErrs, len(perTicketErrs) > 0
}

// deduplicateTicketIDs returns a new slice containing unique ticket IDs from ids,
// preserving first-seen order.
func deduplicateTicketIDs(ids []string) []string {
	if len(ids) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(ids))
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if _, exists := seen[id]; !exists {
			seen[id] = struct{}{}
			out = append(out, id)
		}
	}
	return out
}

// ActivateTicketsWithResults takes a channel of ticket ids for tickets awaiting
// activation, and breaks them into batches of the size defined in
// rc.Cfg.GetInt("OM_CORE_MAX_UPDATES_PER_ACTIVATION_CALL") (which must be <=
// om-core's OM_MAX_STATE_UPDATES_PER_CALL, and also bounds packed stream entries
// when OM_CACHE_PACK_TICKET_STATE_UPDATES=true).
//
// Per-ticket error details (google.rpc.BadRequest field violations) returned by
// om-core are parsed so that permanent InvalidArgument (HTTP 400) errors are
// not retried and partial Internal (HTTP 500) failures retry only the failed
// ticket subset. An ActivationResult is emitted to resultsChan for each ticket ID
// once its activation succeeds or permanently fails; resultsChan must be non-nil
// and drained by the caller.
func (rc *RestfulOMGrpcClient) ActivateTicketsWithResults(ctx context.Context, ticketIdsToActivate chan string, resultsChan chan<- ActivationResult) {
	logger := rc.Log.WithFields(logrus.Fields{
		"component": "open_match_client",
		"operation": "proxy_ActivateTickets",
		"caller": func() string {
			// Quick in-line function to grab information the calling function
			// put into the context and propogate it to the logs
			ts, ok := ctx.Value("activationType").(string)
			if !ok {
				rc.Log.Error("unable to get caller type from context")
				return "undefined"
			}
			return ts
		}(),
	})

	emitResult := func(id string, err error) {
		select {
		case resultsChan <- ActivationResult{TicketId: id, Err: err}:
		case <-ctx.Done():
		}
	}

	// batching asynchronous goroutine. Processes incoming ticket ids into a
	// batch of max size OM_CORE_MAX_UPDATES_PER_ACTIVATION_CALL, or until the
	// deadline is reached, whichever comes first, then starts a new batch as long
	// as the incoming channel has not been closed.
	batchChan := make(chan []string)
	go func() {
		bLogger := logger.WithFields(logrus.Fields{
			"suboperation": "create_batches",
		})

		maxBatchSize := rc.Cfg.GetInt("OM_CORE_MAX_UPDATES_PER_ACTIVATION_CALL")

		// As long as the channel is not closed and context is not cancelled, keep reading batches from it.
		numBatches := 0
		senderActive := true
		var tid string
		for senderActive {

			// Wait no longer than deadline for a batch to be created and sent for processing
			deadline := time.NewTimer(250 * time.Millisecond)
			activationBatch := make([]string, 0)
			batchComplete := false

			// Loop through incoming ticket ids, until batch limit is reached, deadline is exceeded, or ctx is cancelled
			for !batchComplete {

				select {
				case <-ctx.Done():
					senderActive = false
					batchComplete = true
				case tid, senderActive = <-ticketIdsToActivate:
					if !senderActive {
						batchComplete = true
						break
					}
					activationBatch = append(activationBatch, tid)
					if maxBatchSize > 0 && len(activationBatch) == maxBatchSize {
						batchComplete = true
					}
				case <-deadline.C:
					batchComplete = true
				}
			}
			deadline.Stop()

			// Send the batch to be sent in a single call to om-core
			if len(activationBatch) > 0 {
				numBatches++
				bLogger.Tracef("queueing batch of %v ticket ids to send for activation", len(activationBatch))
				batchChan <- activationBatch
			}

		}

		// All done with creating batches.
		bLogger.Tracef("processed %v batches before the sender stopped sending ticket ids", numBatches)
		close(batchChan)
	}()

	// Loop that sends all batches made by the above goroutine to om-core
	var activationWg sync.WaitGroup
	for batch := range batchChan {

		// Track this goroutine before launching to prevent racing with activationWg.Wait()
		activationWg.Add(1)

		// Kick off activation in a goroutine, so if there are multiple batches
		// all calls are made concurrently.
		go func(thisBatch []string) {
			defer activationWg.Done()

			pendingBatch := deduplicateTicketIDs(slices.Clone(thisBatch))

			// HTTP version of the gRPC ActivateTickets() call that we want to retry with exponential backoff and jitter
			activateTicketCall := func() error {
				if len(pendingBatch) == 0 {
					return nil
				}

				reqPb := &pb.ActivateTicketsRequest{TicketIds: pendingBatch}
				req, err := protojson.Marshal(reqPb)
				if err != nil {
					logger.WithFields(logrus.Fields{
						"pb_message": "ActivateTicketsRequest",
					}).Errorf("cannot marshal protobuf to json")
					return backoff.Permanent(err)
				}

				logger.Tracef("ActivateTicket call with %v tickets: %v...", len(pendingBatch), pendingBatch[0])
				resp, err := rc.Post(ctx, logger, rc.Cfg.GetString("OM_CORE_ADDR"), "/tickets:activate", req)
				var body []byte
				if resp != nil && resp.Body != nil {
					body, _ = io.ReadAll(resp.Body)
					resp.Body.Close()
				}

				if err != nil {
					logger.Errorf("ActivateTickets attempt failed: %v", err)
					return err
				}
				if resp.StatusCode == http.StatusOK {
					for _, id := range pendingBatch {
						emitResult(id, nil)
					}
					pendingBatch = nil
					return nil
				}

				code, perTicketErrs, hasDetails := parseTicketStatusBody(body)
				switch {
				case hasDetails && (resp.StatusCode == http.StatusBadRequest || code == codes.InvalidArgument):
					// om-core already activated the valid IDs in the batch and returned
					// BadRequest field violations for the invalid/failed ones. Note that if a batch
					// contained both invalid IDs and valid IDs that suffered a transient state-update
					// failure, om-core merges both into BadRequest field violations and returns
					// InvalidArgument (HTTP 400). Distinguish transient Internal state-update errors
					// so they are still retried.
					stillPending := make([]string, 0)
					numInvalid := 0
					for _, id := range pendingBatch {
						if errMsg, failed := perTicketErrs[id]; failed {
							if strings.Contains(errMsg, "Unable to update ticket") || strings.Contains(errMsg, "code = Internal") {
								stillPending = append(stillPending, id)
							} else {
								numInvalid++
								logger.WithField("ticket_id", id).Errorf("ActivateTickets permanent failure for ticket: %s", errMsg)
								emitResult(id, errors.New(errMsg))
							}
						} else {
							emitResult(id, nil)
						}
					}
					if len(stillPending) > 0 {
						pendingBatch = stillPending
						err = fmt.Errorf("ActivateTickets failed for %d invalid ticket(s) and %d retryable ticket(s): %s (%d)", numInvalid, len(stillPending), http.StatusText(resp.StatusCode), resp.StatusCode)
						logger.Errorf("ActivateTickets attempt failed: %v", err)
						return err
					}
					pendingBatch = nil
					return backoff.Permanent(fmt.Errorf("ActivateTickets failed for %d invalid ticket(s)", numInvalid))

				case !hasDetails && (resp.StatusCode == http.StatusBadRequest || code == codes.InvalidArgument):
					// om-core returns InvalidArgument without details when 100% of IDs in the batch are invalid.
					permErr := fmt.Errorf("ActivateTickets permanent failure: %s (%d)", http.StatusText(resp.StatusCode), resp.StatusCode)
					logger.Errorf("ActivateTickets attempt failed: %v", permErr)
					for _, id := range pendingBatch {
						emitResult(id, permErr)
					}
					pendingBatch = nil
					return backoff.Permanent(permErr)

				case hasDetails && (resp.StatusCode == http.StatusInternalServerError || code == codes.Internal):
					// Tickets not present in perTicketErrs succeeded; retry only the failed subset.
					stillPending := make([]string, 0, len(perTicketErrs))
					for _, id := range pendingBatch {
						if _, failed := perTicketErrs[id]; failed {
							stillPending = append(stillPending, id)
						} else {
							emitResult(id, nil)
						}
					}
					pendingBatch = stillPending
					err = fmt.Errorf("ActivateTickets failed for %d ticket(s): %s (%d)", len(pendingBatch), http.StatusText(resp.StatusCode), resp.StatusCode)
					logger.Errorf("ActivateTickets attempt failed: %v", err)
					return err

				default:
					err = fmt.Errorf("%s (%d)", http.StatusText(resp.StatusCode), resp.StatusCode)
					logger.Errorf("ActivateTickets attempt failed: %v", err)
					return err
				}
			}

			// Try to call ActivateTickets() with retries for up to 5 seconds, respecting context cancellation.
			err := backoff.RetryNotify(
				activateTicketCall,
				backoff.WithContext(backoff.NewExponentialBackOff(backoff.WithMaxElapsedTime(5*time.Second)), ctx),
				func(err error, bo time.Duration) {
					logger.Errorf("ActivateTicket temporary failure (backoff for %v): %v", err, bo)
				},
			)
			if err != nil {
				logger.Errorf("ActivateTickets failed: %v", err)
				for _, id := range pendingBatch {
					emitResult(id, err)
				}
			}
			logger.Trace("ActivateTickets complete")
		}(batch)
	}

	// wait for all outstanding asynchronous calls to om-core's ActivateTickets() endpoint to complete
	activationWg.Wait()
}

func (rc *RestfulOMGrpcClient) InvokeMatchmakingFunctions(ctx context.Context, reqPb *pb.MmfRequest, respChan chan *pb.StreamedMmfResponse) {
	// Make sure the output channel always gets closed when this function is finished.
	defer close(respChan)

	logger := rc.Log.WithFields(logrus.Fields{
		"component": "open_match_client",
		"operation": "proxy_InvokeMatchmakingFunctions",
		"url":       rc.Cfg.GetString("OM_CORE_ADDR") + "/matches:fetch",
	})

	// Have to cancel the context to tell the om-core server we're done reading from the stream.
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(MMFsComplete)

	// Marshal protobuf request to JSON for grpc-gateway HTTP call
	req, err := protojson.Marshal(reqPb)
	if err != nil {
		logger.WithFields(logrus.Fields{
			"pb_message": "MmfRequest",
			"error":      err,
		}).Errorf("cannot marshal protobuf to json")
		return
	}
	logger.Trace("Marshalled MmfRequest protobuf message to JSON")

	// HTTP version of gRPC InvokeMatchmakingFunctions() call
	logger.Trace("Calling InvokeMatchmakingFunctions()")
	resp, err := rc.Post(ctx, logger, rc.Cfg.GetString("OM_CORE_ADDR"), "/matches:fetch", req)
	if err != nil {
		logger.Errorf("InvokeMatchmakingFunction call failed: %v", err)
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
		return
	}
	if resp == nil || resp.Body == nil {
		logger.Error("InvokeMatchmakingFunction call returned nil HTTP response")
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, resp.Body)
		logger.Errorf("InvokeMatchmakingFunction call failed: %v", resp.StatusCode)
		return
	}

	logger.Tracef("InvokeMatchmakingFunction call successful: %v", resp.StatusCode)

	// Process the result stream line-by-line. Use bufio.Scanner instead of
	// fmt.Fscanf("%s\n") because protojson-serialized extensions may contain
	// spaces inside JSON string values.
	// Raise bufio.Scanner's maximum token size from the 64 KiB default
	// (bufio.MaxScanTokenSize) to 10 MiB because om-core supports streamed MMF
	// responses up to 4 MiB in protobuf, which can expand significantly when
	// serialized as a single newline-delimited JSON line with base64-encoded
	// google.protobuf.Any extensions.
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 10*1024*1024)
	for scanner.Scan() {
		msg := strings.TrimSpace(scanner.Text())
		if msg == "" {
			continue
		}

		// grpc-gateway wraps each streamed message in a top-level JSON envelope:
		//   - {"result": <StreamedMmfResponse JSON>} for normal match responses, or
		//   - {"error": {"code": <int>, "message": "<string>", ...}} if an RPC
		//     error occurs mid-stream after HTTP 200 headers were already sent.
		// Parse the outer envelope first so mid-stream RPC errors are logged
		// cleanly instead of being misparsed as empty StreamedMmfResponse messages.
		var envelope struct {
			Result json.RawMessage `json:"result"`
			Error  *struct {
				Code    int    `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
		}
		payload := []byte(msg)
		if jsonErr := json.Unmarshal(payload, &envelope); jsonErr == nil {
			if envelope.Error != nil {
				logger.WithFields(logrus.Fields{
					"code":  envelope.Error.Code,
					"error": envelope.Error.Message,
				}).Errorf("InvokeMatchmakingFunction stream returned error: %s (code %d)", envelope.Error.Message, envelope.Error.Code)
				continue
			}
			if len(envelope.Result) > 0 {
				payload = envelope.Result
			}
		}

		logger.WithFields(logrus.Fields{
			"pb_message": "StreamedMmfResponse",
		}).Tracef("HTTP response JSON body version of StreamedMmfResponse received")

		// Unmarshal json back into protobuf
		resPb := &pb.StreamedMmfResponse{}
		err = protojson.Unmarshal(payload, resPb)
		if err != nil {
			logger.WithFields(logrus.Fields{
				"pb_message":    "StreamedMmfResponse",
				"http_response": string(payload),
				"error":         err,
			}).Errorf("cannot unmarshal http response body back into protobuf")
			continue
		}
		logger.Trace("Successfully unmarshalled HTTP response JSON body back into StreamedMmfResponse protobuf message")

		if resPb.GetMatch() == nil {
			logger.Trace("StreamedMmfResponse protobuf was empty!")
			continue // Loop again to get the next streamed response
		}

		// Send back the streamed responses as we get them
		logger.Trace("StreamedMmfResponse protobuf exists and is not empty")
		select {
		case respChan <- resPb:
		case <-ctx.Done():
			return
		}
	}
	// scanner.Scan() returns false both on clean EOF (where scanner.Err() is nil)
	// and on stream read or token errors (such as bufio.ErrTooLong or a mid-stream
	// network failure). Check ctx.Err() == nil so expected context cancellation
	// does not log a spurious stream read error.
	if err := scanner.Err(); err != nil && !errors.Is(err, io.EOF) && ctx.Err() == nil {
		logger.Errorf("Error reading stream, closing: %v", err)
	}
}

func (c *idTokenCache) get(audience string) (*oauth2.Token, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, ok := c.entries[audience]; ok {
		if entry.token != nil && entry.token.Valid() {
			return entry.token, nil
		}
		tok, err := entry.source.Token()
		if err == nil && tok != nil && tok.Valid() {
			entry.token = tok
			return tok, nil
		}
		delete(c.entries, audience)
	}
	return c.mintFreshLocked(audience)
}

func (c *idTokenCache) refresh(audience string, rejectedAccessToken string) (*oauth2.Token, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, ok := c.entries[audience]; ok {
		if entry.token != nil && entry.token.AccessToken != rejectedAccessToken && entry.token.Valid() {
			return entry.token, nil
		}
		delete(c.entries, audience)
	}
	return c.mintFreshLocked(audience)
}

func (c *idTokenCache) mintFreshLocked(audience string) (*oauth2.Token, error) {
	// Use context.Background() so short-lived per-RPC contexts (e.g. CreateTicket)
	// do not cancel the underlying shared TokenSource.
	src, err := c.mint(context.Background(), audience)
	if err != nil {
		return nil, fmt.Errorf("Cloud Run service account authentication requires token source, but couldn't get one. idtoken.NewTokenSource: %v", err)
	}
	tok, err := src.Token()
	if err != nil {
		return nil, fmt.Errorf("Cloud Run service account authentication requires a token, but couldn't get one. TokenSource.Token: %v", err)
	}
	c.entries[audience] = &cachedIDToken{
		source: src,
		token:  tok,
	}
	return tok, nil
}

// Post makes an HTTP request at the given url+path, and a byte buffer of the
// data to POST.  It attempts to transparently handle TLS if the target server
// requires it.
func (rc *RestfulOMGrpcClient) Post(ctx context.Context, logger *logrus.Entry, url string, path string, reqBuf []byte) (*http.Response, error) {
	// Add the url as a structured logging field when debug logging is enabled
	postLogger := logger.WithFields(logrus.Fields{
		"url": url + path,
	})

	// Set up our request parameters
	req, err := http.NewRequestWithContext(
		ctx,                     // Context
		http.MethodPost,         // HTTP verb
		url+path,                // RESTful OM2 path
		bytes.NewReader(reqBuf), // JSON-marshalled protobuf request message
	)
	if err != nil {
		postLogger.Errorf("cannot create http request with context")
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	postLogger.Trace("Header set, request created")

	onCloudRun := os.Getenv("K_REVISION") != ""
	var token *oauth2.Token
	if onCloudRun { // Running on Cloud Run, get GCP SA token
		token, err = rc.tokens.get(url)
		if err != nil {
			return nil, err
		}
		token.SetAuthHeader(req)
	}

	// Log all request headers if using trace logging (slow)
	if logrus.IsLevelEnabled(logrus.TraceLevel) {
		headerFields := logrus.Fields{}
		for key, value := range req.Header {
			headerFields[key] = value
		}
		postLogger.WithFields(headerFields).Debug("Prepared Headers")
	}

	// Send request
	resp, err := rc.Client.Do(req)
	if err != nil {
		postLogger.WithFields(logrus.Fields{
			"error": err,
		}).Error("cannot execute http request")
		return nil, err
	}

	// If the server rejected the cached Cloud Run ID token with 401 Unauthorized,
	// refresh the token from a fresh TokenSource and retry once.
	if onCloudRun && token != nil && resp != nil && resp.StatusCode == http.StatusUnauthorized {
		if resp.Body != nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
		refreshedToken, refreshErr := rc.tokens.refresh(url, token.AccessToken)
		if refreshErr != nil {
			return nil, refreshErr
		}
		retryReq, reqErr := http.NewRequestWithContext(
			ctx,
			http.MethodPost,
			url+path,
			bytes.NewReader(reqBuf),
		)
		if reqErr != nil {
			postLogger.Errorf("cannot create retry http request with context")
			return nil, reqErr
		}
		retryReq.Header.Set("Content-Type", "application/json")
		refreshedToken.SetAuthHeader(retryReq)
		resp, err = rc.Client.Do(retryReq)
		if err != nil {
			postLogger.WithFields(logrus.Fields{
				"error": err,
			}).Error("cannot execute retry http request")
			return nil, err
		}
	}
	return resp, err
}

// readAllBody is a simple helper function to make sure an HTTP body is completely read and closed.
func readAllBody(resp http.Response, logger *logrus.Entry) ([]byte, error) {
	// Get results
	body, err := io.ReadAll(resp.Body)
	defer resp.Body.Close()
	if err != nil {
		logger.Errorf("cannot read bytes from http response body")
		return nil, err
	}

	return body, err
}

// ValidateConnection checks that RestfulOMGrpcClient can reach and authenticate
// against the provided Open Match core URL without writing dummy tickets to state
// storage.
//
// It probes "POST /tickets:activate" with an empty ActivateTicketsRequest. On a
// live om-core instance, ActivateTickets validates the empty request in memory
// and returns HTTP 400 Bad Request (codes.InvalidArgument) without touching Redis,
// logging errors, or recording ticket metrics; stub test servers may also return
// HTTP 200 OK. Any other HTTP status code (such as 401/403 auth rejection, 404
// wrong route, or 5xx server failure) or transport error is returned as an error,
// and resp.Body is always drained and closed so the underlying connection can be
// reused by http.Transport.
func (rc *RestfulOMGrpcClient) ValidateConnection(ctx context.Context, url string) error {
	buf, err := protojson.Marshal(&pb.ActivateTicketsRequest{})
	if err != nil {
		return err
	}

	resp, err := rc.Post(ctx,
		rc.Log.WithFields(logrus.Fields{
			"operation": "proxy_ValidateConnection",
		}),
		url, "/tickets:activate", buf)
	if resp != nil && resp.Body != nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	if err != nil {
		return err
	}
	if resp == nil {
		return errors.New("ValidateConnection received nil HTTP response")
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusBadRequest {
		return fmt.Errorf("ValidateConnection unexpected HTTP status: %s (%d)", http.StatusText(resp.StatusCode), resp.StatusCode)
	}
	return nil
}

// syncMapDump can be uncommented and called to see the contents of a sync.Map
// when debugging.
//func syncMapDump(sm *sync.Map) map[string]interface{} {
//	out := map[string]interface{}{}
//	sm.Range(func(key, value interface{}) bool {
//		out[fmt.Sprint(key)] = value
//		return true
//	})
//	return out
//}
