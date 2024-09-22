package main

import (
	"encoding/json"
	"fmt"
	resty "github.com/go-resty/resty/v2"
	"github.com/samber/lo"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"time"
)

var logger = log.New(os.Stdout, "", log.LstdFlags)

const DEBUG = true

func main() {
	projectID := os.Getenv("TRACKER_PROJECT")
	trackerToken := os.Getenv("TRACKER_TOKEN")
	if projectID == "" || trackerToken == "" {
		logger.Fatalf("supply TRACKER_PROJECT and TRACKER_TOKEN as env variables first")
	}

	client := resty.New()
	client.SetBaseURL("https://www.pivotaltracker.com/services/v5/projects/" + projectID)
	client.Header.Set("X-TrackerToken", trackerToken)

	simpleListFetcher := func(path string) ContentsFetcher[[]genericJsonObject] {
		return func() []genericJsonObject {
			return fetchSimpleList(client, path)
		}
	}
	_ = simpleListFetcher
	paginatedListFetcher := func(path string) ContentsFetcher[[]genericJsonObject] {
		return func() []genericJsonObject {
			return fetchPaginated(client, path)
		}
	}
	_ = paginatedListFetcher
	subitemsFetcher := func(parents []genericJsonObject, path string, parentParam string) ContentsFetcher[[]Subitems] {
		return func() []Subitems {
			return getSubItems(client, parents, path, parentParam)
		}
	}
	_ = subitemsFetcher

	var stories []genericJsonObject
	savingFetcher(persistJson(paginatedListFetcher("/stories"), "stories.json"), &stories)()

	savingFetcher(persistJson(paginatedListFetcher("/stories"), "stories.json"), &stories)()
	persistJson(subitemsFetcher(stories, "/stories/{storyID}/tasks", "storyID"), "story_tasks.json")()
	persistJson(subitemsFetcher(stories, "/stories/{storyID}/owners", "storyID"), "story_owners.json")()
	persistJson(subitemsFetcher(stories, "/stories/{storyID}/comments", "storyID"), "story_comments.json")()
	persistJson(subitemsFetcher(stories, "/stories/{storyID}/reviews", "storyID"), "story_reviews.json")()
	persistJson(subitemsFetcher(stories, "/stories/{storyID}/blockers", "storyID"), "story_blockers.json")()

	persistJson(paginatedListFetcher("/story_transitions"), "story_transitions.json")()
	persistJson(paginatedListFetcher("/iterations"), "iterations.json")()

	var releases []genericJsonObject
	savingFetcher(persistJson(paginatedListFetcher("/releases"), "releases.json"), &releases)()
	persistJson(subitemsFetcher(releases, "/releases/{id}/stories", "id"), "releases_stories.json")()
	persistJson(simpleListFetcher("/labels"), "labels.json")()

	var epics []genericJsonObject
	savingFetcher(persistJson(simpleListFetcher("/epics"), "epics.json"), &epics)()
	persistJson(subitemsFetcher(epics, "/epics/{epic_id}/comments", "epic_id"), "epic_comments.json")()

	persistJson(simpleListFetcher("/memberships"), "memberships.json")()

	completionChecker.report()

}

type completionEntry struct {
	expr     *regexp.Regexp
	seen     bool
	template string
}

// tracks if we processed all endpoints in wantedTrackerAPIEndpoints
type CompletionChecker struct {
	items []*completionEntry
}

func (c *CompletionChecker) observe(url string) {
	for _, i := range c.items {
		if i.seen {
			continue
		}
		if i.expr.FindStringIndex(url) != nil {
			logger.Println("completion checker: registering as observed: ", i.template)
			i.seen = true
			break
		}
	}
}

func (c *CompletionChecker) report() {
	done := true
	for _, i := range c.items {
		if i.seen {
			continue
		}
		logger.Println("completion checker: not done yet: ", i.template)
		done = false
	}
	if done {
		logger.Println("completion checker: all done.")
	}
}

func newCompletionChecker() *CompletionChecker {
	return &CompletionChecker{
		items: lo.Map(wantedTrackerAPIEndpoints, func(urlTemplate string, index int) *completionEntry {
			substitutePathParams := regexp.MustCompile(`({[^/]+)}`)
			urlRegexp := substitutePathParams.ReplaceAllString(urlTemplate, "[^/]+") + `($|\?.*)`

			return &completionEntry{
				expr:     regexp.MustCompile(urlRegexp),
				seen:     false,
				template: urlTemplate,
			}
		}),
	}
}

var completionChecker = newCompletionChecker()

type ContentsFetcher[T any] func() T

func savingFetcher[T any](fetcher ContentsFetcher[T], cache *T) ContentsFetcher[T] {
	return func() T {
		results := fetcher()
		*cache = results
		return results
	}
}

func persistJson[T any](fetcher ContentsFetcher[T], destinationFile string) ContentsFetcher[T] {
	return func() T {
		existingFileContents, err := os.ReadFile(destinationFile)
		if err == nil {
			var contents T
			err := json.Unmarshal(existingFileContents, &contents)
			if err == nil {
				logger.Printf("file %q exists, reusing cached contents", destinationFile)
				return contents
			}
		}

		logger.Println("fetching contents for", destinationFile)
		contents := fetcher()
		marshalledContents, _ := json.MarshalIndent(contents, "", "  ")
		os.WriteFile(destinationFile, marshalledContents, 0644)
		logger.Println("file created:", destinationFile)
		return contents
	}
}

type genericJsonObject map[string]any

func fetchPaginated(client *resty.Client, path string) []genericJsonObject {
	type paginatedResponse struct {
		Pagination struct {
			Total    int `json:"total"`
			Limit    int `json:"limit"`
			Offset   int `json:"offset"`
			Returned int `json:"returned"`
		} `json:"pagination"`
		Data []genericJsonObject `json:"data"`
	}

	var result []genericJsonObject
	done := false
	for !done {
		respBody := paginatedResponse{}

		getWithRetries(client.R().
			SetResult(&respBody).
			SetQueryParam("offset", fmt.Sprintf("%d", len(result))).
			SetQueryParam("envelope", "true"), path)

		result = append(result, respBody.Data...)
		logger.Printf("fetched %d/%d\n", len(result), respBody.Pagination.Total)
		done = len(result) == respBody.Pagination.Total
		if DEBUG {
			break
		}
	}
	return result
}

type requestFn func(*resty.Request)

func withPathParam(key string, value string) requestFn {
	return func(r *resty.Request) {
		r.SetPathParam(key, value)
	}
}

func fetchSimpleList(client *resty.Client, path string, requestFn ...requestFn) []genericJsonObject {
	respBody := []genericJsonObject{}

	req := client.R().SetResult(&respBody)

	getWithRetries(req, path, requestFn...)
	return respBody
}

type Subitems struct {
	ParentID int64
	Items    []genericJsonObject
}

func getSubItems(client *resty.Client, parents []genericJsonObject, path string, parentParam string) []Subitems {
	//goland:noinspection GoBoolExpressions
	if DEBUG && len(parents) > 3 {
		parents = parents[:3]
	}
	return lo.FilterMap(parents, func(item genericJsonObject, index int) (Subitems, bool) {
		parentID := int64(item["id"].(float64))
		fmt.Printf("\rfetching items (%d/%d), parent ID %d", index+1, len(parents), parentID)

		items := fetchSimpleList(client, path, withPathParam(parentParam, strconv.FormatInt(parentID, 10)))
		return Subitems{
			ParentID: parentID,
			Items:    items,
		}, len(items) > 0
	})
}

func getWithRetries(req *resty.Request, path string, requestFn ...requestFn) {
	for _, fn := range requestFn {
		fn(req)
	}
	for {

		resp, err := req.Get(path)
		if err != nil {
			log.Println("API call failed, will retry")
			time.Sleep(5 * time.Second)
			continue
		}

		completionChecker.observe(resp.Request.URL)

		switch resp.StatusCode() {
		case http.StatusOK:
			return
		case http.StatusTooManyRequests:
			logger.Println("server complaining about too many requests, sleeping and retrying in a bit")
			time.Sleep(5 * time.Second)
		default:
			logger.Fatalf("unexpected status (%s): %v", req.RawRequest.URL.String(), resp.Status())
		}
	}
}
