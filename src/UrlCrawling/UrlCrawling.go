package UrlCrawling

import (
   "fmt"
   "golang.org/x/net/html"
   "net/http"
   "net/url"
   "strings"
   "sync"
)

const LINKS_LEVEL = 2 //Crawling depth

/* Map between URLs (keys) and their data/images (values) */
type MapUrlsData struct {
   sync.Mutex
   UrlsData map[string]map[string]string
}

/* Slice of URLs */
type Urls struct {
   sync.Mutex
   Urls map[string]string
}

/* Processing Info related to a specific URL:
- DomainUrl: parsed URL of the specific URL,
- WaitingUrls: related URLs waiting to be crawled, added when the specific URL and its related URLs are crawled,
- CompletedUrls: related URLs crawled among those previously in the WaitingUrls set,
- ProcessingUrls: related URLs being crawled, so not belonging to the WaitingUrls set anymore, and not yet belonging to the CompletedUrls set.*/
type UrlProcess struct {
   sync.Mutex
   DomainUrl *url.URL
   WaitingUrls *Urls
   CrawledUrls *MapUrlsData
   ProcessingUrls *MapUrlsData
}



// Helper function to get the value of the specified tag from a Token
func getTokenValue(token html.Token, tag string) (tagFound bool, val string) {
   // Iterate over token attributes until we find the specified tag
   for _, att := range token.Attr {
      if att.Key == tag {
         val = att.Val
         tagFound = true
      }
   }
   return tagFound, val
}

/* Crawling a URL page.
This method shall crawl the specified URL to get its data (images) and new URLs to crawl.
It shall read the content body of the specified URL, and terminates the function if an error is raised or if the end of URL is reached.
Else, it shall go through the specified URL and:
- add found URLs to the waiting URLs set of the specified receiver urlProcess if the following conditions are met:
   - links grabing is enabled, indeed if at least one of the below conditions is met:
      - the specified URL is the same as the reference URL given by the specified receiver urlProcess,
      - the specified URL is maximum level 2, relatively to the reference URL.
   - the found URLs are not already part of the crawled URLs nor processing URLs set of the specified receiver urlProcess,
   - the found URLs have the same host value as the reference URL's one.
- add found data (images) for the specified URL in the processing URLs set of the specified receiver urlProcess if:
   - the found data have not been added for the specified URL yet,
   - the found data are images that have the following extensions only: .png, .gif or .jpeg.
*/
func (urlProcess *UrlProcess) CrawlUrl(urlToCrawl *string) { 
   // Reading URL content body, and leaving the function if an error is raised.
   urlContent, err := http.Get(*urlToCrawl)
   if err != nil {
      fmt.Println("ERROR: Failed to crawl: ", *urlToCrawl)
      return
   }
   urlBody := urlContent.Body
   defer urlBody.Close()

   refUrl := urlProcess.DomainUrl

   collectLinksEnable := false
   urlExtension := strings.SplitAfter(*urlToCrawl, refUrl.String())
   // Collecting links if the crawling URL is the reference URL or if the URL level is maximum 2
   if( (*urlToCrawl == refUrl.String()) || (len(strings.Split(urlExtension[len(urlExtension)-1], "/")) < LINKS_LEVEL) ) {
      collectLinksEnable = true
   }

   // Looping on all tokens found in the crawled ULR
   urlTokenizer := html.NewTokenizer(urlBody)
   for {
      tokenizeItem := urlTokenizer.Next()
      switch {
         case tokenizeItem == html.ErrorToken:
            // Case where the current token means end of the URL-> ending the crawling task
            return
         case tokenizeItem == html.StartTagToken:
            // Case where the current token is a html tag
            token := urlTokenizer.Token() 
            if((token.Data == "a")||(token.Data == "link")) {
               // Checking if the tag is an <a> or a <link> tag
               if collectLinksEnable { 
                  // Extracting the link if needed
                  hasLink, link := getTokenValue(token, "href")
                  if hasLink {
                     // If exisiting, parsing the link to the absolute URL path
                     parsedUrl, errParse := ParseUrl(&link)
                     if(errParse == nil){
                        linkAbs := refUrl.ResolveReference(parsedUrl)
                        // Checking that the parsed URL link is not in the crawling URLs set neither processing URLs set from the receiver specified URL process
                        crawledUrls := urlProcess.CrawledUrls
                        processingUrls := urlProcess.ProcessingUrls
                        processingUrls.Lock()
                        _, alreadyProcessing := processingUrls.UrlsData[linkAbs.String()]
                        processingUrls.Unlock()
                        crawledUrls.Lock()
                        _, alreadyCrawled := crawledUrls.UrlsData[linkAbs.String()]
                        crawledUrls.Unlock()
                        if (!alreadyCrawled && !alreadyProcessing) {
                           // If the parsed URL link never seen yet, checking if the host of the parsed URL is the same as the one of the reference URL
                           if((linkAbs.Host == refUrl.Host)){ 
                              // Adding the parsed URL link as a new key in the waiting URLs set from the receiver specified URL process (no need to have a value associated to this key)
                              waitingUrls := urlProcess.WaitingUrls
                              waitingUrls.Lock()
                              waitingUrls.Urls[linkAbs.String()] = "" 
                              waitingUrls.Unlock()     
                           }  
                        }
                     }
                  }    
               }
            } else if (token.Data == "img") {
               // Checking if the tag is an <img> tag
               hasImg, img := getTokenValue(token, "src")
               // Extracting the image path
               if hasImg {
                  // If exisiting, parsing the image path to the absolute URL path
                  parsedUrl, errParse := ParseUrl(&img)
                  if(errParse == nil){
                     imgAbs := refUrl.ResolveReference(parsedUrl)
                     // Checking if image extension is .png, .gif or .jpeg
                     imgAbsSplit := strings.Split(imgAbs.String(), ".")
                     imgAbsExtension := imgAbsSplit[len(imgAbsSplit)-1] 
                     if((imgAbsExtension == "png") || (imgAbsExtension == "gif") || (imgAbsExtension == "jpeg")){
                        // Adding the parsed URL link to the specified crawled URLs
                        processingUrls := urlProcess.ProcessingUrls
                        processingUrls.Lock()
                        if _, alreadySeen := processingUrls.UrlsData[*urlToCrawl][imgAbs.String()]; !alreadySeen {
                           // Adding the parsed URL link if not seen yet during the on-going crawling as a new key in the processing URLs set from the receiver specified URL process (no need to have a value associated to this key)
                           processingUrls.UrlsData[*urlToCrawl][imgAbs.String()] = ""    
                        }
                        processingUrls.Unlock()          
                     }
                  }
               }
            }
      } 

   }

}

/* URL parsing.
This method shall parse the specified URL and return the parsed URL with the associated error.
*/
func ParseUrl(urlToParse *string) (*url.URL, error) {
   urlParsed, err := url.Parse(*urlToParse)
   return urlParsed, err
}

/* UrlProcess initialization.
This method shall initialize a UrlProcess by:
- assigning the parsed specified URL to the DomainUrl parameter,
- initializing the waitingUrls parameter with the specified URL only (no data): will be used as a set to store all the URLs related to the specified URL, waiting to be crawled,
- initializing the processingUrls parameter empty (no URL nor data): will be used as a set to store all the URLs related to the specified URL, being crawled,
- initializing the crawledUrls parameter empty (no URL nor data): will be used as a set to store all the URLs related to the specified URL, already crawled.
*/
func (urlProcess *UrlProcess) InitUrlProcess(urlToParse *string) {
   // Assigning the parsed specified URL
   parsedUrl, _ := ParseUrl(urlToParse)
   urlProcess.DomainUrl = parsedUrl
   // Initializing the waitingUrls 
   waitingUrls := &Urls{Urls:map[string]string{*urlToParse:""}}
   urlProcess.WaitingUrls = waitingUrls
   // Initializing the processingUrls 
   processingUrls := &MapUrlsData{UrlsData:map[string]map[string]string{}}
   urlProcess.ProcessingUrls = processingUrls
   // Initializing the crawledUrls 
   crawledUrls := &MapUrlsData{UrlsData:map[string]map[string]string{}}
   urlProcess.CrawledUrls = crawledUrls
}

