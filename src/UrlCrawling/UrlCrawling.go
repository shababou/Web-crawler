package UrlCrawling

import (
   "fmt"
   "golang.org/x/net/html"
   "net/http"
   "net/url"
   "strings"
   "sync"
)

const LINKS_LEVEL = 2

/* Map between each specific URLs (keys) and their children URLs (values) */
type MapUrls struct {
   sync.Mutex
   Urls map[string][]string
}

/* Data related to a specific URL:
- DomainUrl: parsed URL of the specific URL,
- ChildrenUrls: URLs got when crawling the specific URL,
- Data: images found through the specific URL and its children.*/
type UrlData struct {
   sync.Mutex
   DomainUrl *url.URL
   ChildrenUrls map[string]string
   Data []string
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
This method shall crawl the specified URL to get the children crawled URLs and their data.
It shall read the content body of the specified URL, and terminates the function if an error is raised or if the end of URL is reached.
Else, it shall go through the specified URL and:
- add a child crawled URL if the following conditions are met:
   - links grabing is enabled, indeed if at least one of the below conditions is met:
      - the specified URL is the same as the reference URL given by the receiver specified urlData,
      - the specified URL is maximum level 2, relatively to the reference URL.
   - the child crawled URL is not already part of the specified crawledUrls,
   - the child crawled URL has the same host value as the reference URL's one.
- add to the data of the the receiver specified urlData the images that have the following extensions only: .png, .gif or .jpeg.
*/
func (urlData *UrlData) CrawlUrl(urlToCrawl *string, crawledUrls *MapUrls) { 
   // Reading URL content body, and leaving the function if an error is raised.
   urlContent, err := http.Get(*urlToCrawl)
   if err != nil {
      fmt.Println("ERROR: Failed to crawl: ", *urlToCrawl)
      return
   }
   urlBody := urlContent.Body
   defer urlBody.Close()

   refUrl := urlData.DomainUrl

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
                        // Checking if the parsed URL link was already seen on the specified crawled URLs
                        crawledUrls.Lock()
                        if _, alreadyCrawled := crawledUrls.Urls[linkAbs.String()]; !alreadyCrawled{
                           // If not crawled yet, checking if the host of the parsed URL is the same as the one of the reference URL
                           if((linkAbs.Host == refUrl.Host)){ 
                              // Adding the parsed URL link to the specified crawled URLs
                              urlData.Lock()
                              urlData.ChildrenUrls[linkAbs.String()] = "" 
                              urlData.Unlock()     
                           }  
                        }
                        crawledUrls.Unlock()
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
                  if(errParse==nil){
                     imgAbs := refUrl.ResolveReference(parsedUrl)
                     // Checking if image extension is .png, .gif or .jpeg
                     imgAbsSplit := strings.Split(imgAbs.String(), ".")
                     imgAbsExtension := imgAbsSplit[len(imgAbsSplit)-1] 
                     if((imgAbsExtension == "png") || (imgAbsExtension == "gif") || (imgAbsExtension == "jpeg")){
                        // Adding the parsed URL link to the specified crawled URLs
                        urlData.Lock()
                        urlData.Data = append(urlData.Data, imgAbs.String())      
                        urlData.Unlock()          
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

/* UrlData initialization.
This method shall initialize a UrlData by:
- assigning the parsed specified URL to the domainUrl parameter,
- initializing the childrenUrls parameter with the specified URL only,
- initializing the data parameter to empty.
*/
func (urlData *UrlData) InitUrlData(urlToParse *string) {
   // Assigning the parsed specified URL
   parsedUrl, _ := ParseUrl(urlToParse)
   urlData.DomainUrl = parsedUrl
   // Initializing the childrenUrls parameter
   urlData.ChildrenUrls = map[string]string{*urlToParse:""}
   // Initializing the data parameter
   urlData.Data = []string{}
}

