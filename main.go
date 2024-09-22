package main

import (
	"encoding/json"
	"fmt"
	resty "github.com/go-resty/resty/v2"
	"github.com/samber/lo"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"time"
)

var logger = log.New(os.Stdout, "", log.LstdFlags)

const DEBUG = false

func main() {
	outDir := "out"
	if DEBUG {
		outDir = "debug_out"
	}
	err := os.MkdirAll(outDir, 0755)
	if err != nil {
		logger.Fatal(err)
	}
	projectID := os.Getenv("TRACKER_PROJECT")
	trackerToken := os.Getenv("TRACKER_TOKEN")
	if projectID == "" || trackerToken == "" {
		logger.Fatalf("supply TRACKER_PROJECT and TRACKER_TOKEN as env variables first")
	}

	client := resty.New()
	client.SetBaseURL("https://www.pivotaltracker.com/services/v5/projects/" + projectID)
	client.Header.Set("X-TrackerToken", trackerToken)

	simpleList := func(path string) ContentsFetcher[[]genericJsonObject] {
		return func() *FetchedContents[[]genericJsonObject] {
			return fetchSimpleList(client, path)
		}
	}
	paginatedList := func(path string) ContentsFetcher[[]genericJsonObject] {
		return func() *FetchedContents[[]genericJsonObject] {
			return fetchPaginated(client, path)
		}
	}
	subitems := func(parents []genericJsonObject, path string, parentParam string) ContentsFetcher[[]Subitems] {
		return func() *FetchedContents[[]Subitems] {
			return getSubItems(client, parents, path, parentParam)
		}
	}
	out := func(file string) string {
		return filepath.Join(outDir, file)
	}

	{
		var stories []genericJsonObject
		capture(persistJson(paginatedList("/stories"), out("stories.json")), &stories)()
		persistJson(subitems(stories, "/stories/{storyID}/tasks", "storyID"), out("story_tasks.json"))()
		persistJson(subitems(stories, "/stories/{storyID}/owners", "storyID"), out("story_owners.json"))()
		persistJson(subitems(stories, "/stories/{storyID}/comments", "storyID"), out("story_comments.json"))()
		persistJson(subitems(stories, "/stories/{storyID}/reviews", "storyID"), out("story_reviews.json"))()
		persistJson(subitems(stories, "/stories/{storyID}/blockers", "storyID"), out("story_blockers.json"))()
	}

	persistJson(paginatedList("/story_transitions"), out("story_transitions.json"))()
	persistJson(paginatedList("/iterations"), out("iterations.json"))()

	{
		var releases []genericJsonObject
		capture(persistJson(paginatedList("/releases"), out("releases.json")), &releases)()
		persistJson(subitems(releases, "/releases/{id}/stories", "id"), out("releases_stories.json"))()
		persistJson(simpleList("/labels"), out("labels.json"))()
	}

	{
		var epics []genericJsonObject
		capture(persistJson(simpleList("/epics"), out("epics.json")), &epics)()
		persistJson(subitems(epics, "/epics/{epic_id}/comments", "epic_id"), out("epic_comments.json"))()
	}

	persistJson(simpleList("/memberships"), out("memberships.json"))()

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

type FetchedContents[T any] struct {
	Sources   []string
	SavedDate time.Time
	Data      T
}

type ContentsFetcher[T any] func() *FetchedContents[T]

func capture[T any](fetcher ContentsFetcher[T], cache *T) ContentsFetcher[T] {
	return func() *FetchedContents[T] {
		results := fetcher()
		*cache = results.Data
		return results
	}
}

func persistJson[T any](fetcher ContentsFetcher[T], destinationFile string) ContentsFetcher[T] {
	return func() *FetchedContents[T] {
		existingFileContents, err := os.ReadFile(destinationFile)
		if err == nil {
			var contents FetchedContents[T]

			err := json.Unmarshal(existingFileContents, &contents)
			if err == nil {
				logger.Printf("file %q exists, reusing cached contents", destinationFile)
				for _, src := range contents.Sources {
					completionChecker.observe(src)
				}
				return &contents
			}
		}

		logger.Println("fetching contents for", destinationFile)
		contents := fetcher()
		contents.SavedDate = time.Now()

		marshalledContents, _ := json.MarshalIndent(contents, "", "  ")

		if err := os.WriteFile(destinationFile, marshalledContents, 0644); err != nil {
			log.Fatalf("error writing file %q: %v", destinationFile, err)
		}
		logger.Println("file created:", destinationFile)
		return contents
	}
}

type genericJsonObject map[string]any

func fetchPaginated(client *resty.Client, path string) *FetchedContents[[]genericJsonObject] {
	type paginatedResponse struct {
		Pagination struct {
			Total    int `json:"total"`
			Limit    int `json:"limit"`
			Offset   int `json:"offset"`
			Returned int `json:"returned"`
		} `json:"pagination"`
		Data []genericJsonObject `json:"data"`
	}

	var result FetchedContents[[]genericJsonObject]
	done := false
	for !done {
		respBody := paginatedResponse{}

		getWithRetries(&result, client.R().
			SetResult(&respBody).
			SetQueryParam("offset", fmt.Sprintf("%d", len(result.Data))).
			SetQueryParam("envelope", "true"), path)

		result.Data = append(result.Data, respBody.Data...)
		logger.Printf("fetched %d/%d\n", len(result.Data), respBody.Pagination.Total)
		done = len(result.Data) == respBody.Pagination.Total
		if DEBUG {
			break
		}
	}
	return &result
}

type requestFn func(*resty.Request)

func withPathParam(key string, value string) requestFn {
	return func(r *resty.Request) {
		r.SetPathParam(key, value)
	}
}

func fetchSimpleList(client *resty.Client, path string, requestFn ...requestFn) *FetchedContents[[]genericJsonObject] {
	respBody := []genericJsonObject{}

	req := client.R().SetResult(&respBody)

	result := FetchedContents[[]genericJsonObject]{}
	getWithRetries(&result, req, path, requestFn...)

	result.Sources = []string{req.URL}
	result.Data = respBody

	return &result
}

type Subitems struct {
	ParentID int64
	Items    []genericJsonObject
}

func getSubItems(client *resty.Client, parents []genericJsonObject, path string, parentParam string) *FetchedContents[[]Subitems] {
	//goland:noinspection GoBoolExpressions
	if DEBUG && len(parents) > 3 {
		parents = parents[:3]
	}

	result := FetchedContents[[]Subitems]{}

	result.Data = lo.FilterMap(parents, func(item genericJsonObject, index int) (Subitems, bool) {
		parentID := int64(item["id"].(float64))
		fmt.Printf("\rfetching items (%d/%d), parent ID %d", index+1, len(parents), parentID)

		sublist := fetchSimpleList(client, path, withPathParam(parentParam, strconv.FormatInt(parentID, 10)))

		if len(sublist.Data) == 0 {
			return Subitems{}, false
		}

		result.Sources = append(result.Sources, sublist.Sources...)

		return Subitems{
			ParentID: parentID,
			Items:    sublist.Data,
		}, true
	})
	return &result
}

func getWithRetries[T any](result *FetchedContents[T], req *resty.Request, path string, requestFn ...requestFn) {
	for _, fn := range requestFn {
		fn(req)
	}
	var retryTimeout = time.Duration(0)
	for {
		resp, err := req.Get(path)
		if err != nil {
			log.Println("API call failed, will retry")
			time.Sleep(5 * time.Second)
			continue
		}

		completionChecker.observe(resp.Request.URL)
		result.Sources = append(result.Sources, resp.Request.URL)

		switch resp.StatusCode() {
		case http.StatusOK:
			return
		case http.StatusTooManyRequests:
			retryTimeout += 5 * time.Second
			logger.Printf("server complaining about too many requests, sleeping for %s and retrying in a bit", retryTimeout)
			time.Sleep(retryTimeout)

		default:
			logger.Fatalf("unexpected status (%s): %v", req.RawRequest.URL.String(), resp.Status())
		}
	}
}
