# Python 2.7 -- run this script with Python 2.7
import sys, re, time, calendar, grequests

# === read parameters from the command line arguments of the program ===

# Run the script without arguments to get a description of the command line arguments (variables)

# IF the number of arguments is incorrect,
if 10 != len(sys.argv):
    # THEN print to stderr how to use the script
    sys.stderr.write("\n")
    sys.stderr.write("  Usage:\n")
    sys.stderr.write("\n")
    sys.stderr.write("    %s \\\n" % sys.argv[0])
    sys.stderr.write("    <firstUrl>      \\    # url of the first page to be downloaded\n")
    sys.stderr.write("    <urlTimeOut>    \\    # after this number of seconds (1) stop download (2) append URL to end of list to be fetched\n")
    sys.stderr.write("    <nMaxSimultReq> \\    # maximum number of simultaneous requests\n")
    sys.stderr.write("    <sleepTime>     \\    # wait this number of seconds between two groups of requests (below 1 is converted to zero)\n")
    sys.stderr.write("    <urlDomain>     \\    # download pages (nodes) only from this domain\n")
    sys.stderr.write("    <outFileNodes>  \\    # output file listing info about the already downloaded pages (nodes)\n")
    sys.stderr.write("    <outFileLinks>  \\    # output file with info about the already downloaded links (hyperlinks)\n")
    sys.stderr.write("    <flushStdout>   \\    # would you like to flush stdout after each output line? 1: yes, anything else: no\n")
    sys.stderr.write("    <flushOutFiles>      # 1: flush line-by-line to output files, anything else: do not\n")
    sys.stderr.write("\n")
    sys.stderr.write("    # stdout contains for each downloaded page: full link, status code, page size\n")
    sys.stderr.write("    # stderr shows error messages\n")
    sys.stderr.write("    # you should pipe stdout and stderr into separate files / streams\n")
    sys.stderr.write("\n")
    # AND stop the script
    sys.exit()

# Read arguments
_, _firstUrl, _urlTimeOut, _nMaxSimultReq, _sleepTime, _urlDomain, _outFileNodes, _outFileLinks, _flushStdout, _flushOutFiles = sys.argv

# conversions
_urlTimeOut = int(_urlTimeOut)
_nMaxSimultReq = int(_nMaxSimultReq)
_flushStdout = True if '1' == _flushStdout else False
_flushOutFiles = True if '1' == _flushOutFiles else False
_sleepTime = int(_sleepTime) if _sleepTime >= 1.0 else 0

# ========== function definitions ==========

def initDat_initStdout( firstUrl, urlSetAll, urlListFetch, flushStdout ):
    '''Initialize data structures and stdout'''

    # initialize the set of all URLs and the list of those URLs that we would like to download
    urlSetAll.add(firstUrl)
    urlListFetch.append(firstUrl)

    # print header of stdout
    sys.stdout.write("# TAB-separated items: status code, page size (page size is \"-\" if status code is not 200), full link\n")
    sys.stdout.write("\n") # blank line between header and data
    # IF needed, THEN flush stdout
    if flushStdout:
        sys.stdout.flush()
    
# ----------------------------

def fullUrl2urlDir(fullUrl):
    '''From url = http(s)://domain.com/dir/subdir/page extract http(s)://domain.com/dir/subdir'''

    # declare default return value
    urlDir = fullUrl
    # remove the last / and everything after it
    urlDir = re.sub(r'\/[^\/]*?$', '', urlDir)
    # return value
    return urlDir
    
# ----------------------------

def fullUrl2siteUrl(fullUrl):
    '''From url = http(s)://domain.com/page extract http(s)://domain.com'''

    # check the format of the full url:
    # IF it does _not_ start with http(s)://
    if not re.match(r'^https?\:\/{2}', fullUrl):
        # THEN write an error message and exit
        sys.stderr.write("\n\tError in \'fullUrl2siteUrl\': \'fullUrl\' should start with \"http(s)\".\n\tCurrent value: %s\n\n" % fullUrl)
        sys.stderr.flush()
        sys.exit()

    # set default return value
    siteUrl = fullUrl
    # keep only the starting http(s):// and everything until the next / sign
    siteUrl = re.sub(r'^(https?\:\/{2}[^\/]+).*?$', r'\1', siteUrl)

    # return the obtained site URL
    return siteUrl
        
# ----------------------------

def rawUrl2fullUrl(rawUrl, sourceUrl, sourceUrlSite, sourceUrlDir):

    # declare default return value
    fullUrl = rawUrl
    # IF the URL starts with http:// or https://,
    if re.match(r'^https?\:\/{2}', fullUrl):
        # THEN assume that this is a correctly formatted full URL,
        # and we can leave the URL as it is
        pass
    # ELSE IF the target URL starts with a / character followed by a letter or a number,
    elif re.match(r'^\/[a-zA-Z\d]', fullUrl):
        # THEN assume that this is a URL relative to the site of the source URL,
        # so we need to prepend the site of the source URL to it
        fullUrl = sourceUrlSite + fullUrl
    # ELSE IF the target URL starts with a letter or a number
    elif re.match(r'^[a-zA-Z\d]', fullUrl):
        # AND IF it contains a dot before the first / character,
        if re.match(r'^[^\/]+?\.', fullUrl):
            # THEN we assume that this is a full valid URL, and one needs to prepend http:// to it
            fullUrl = "http://" + fullUrl
        # ELSE 
        else:
            # we assume that this is a URL relative to the local directory of the source URL
            fullUrl = sourceUrlDir + '/' + fullUrl
    # in all other cases: discard the URL by blanking it out entirely
    else:
        fullUrl = ''

    # remove trailing / from the full URL
    fullUrl = re.sub(r'\/$', '', fullUrl)

    # return value
    return fullUrl
        
# ----------------------------

def numberThisUrlIfNew(url,url2num,num2url,flushOutFiles,ofn):
    '''If this is a new url, then number it and write to the output file.'''

    # IF we have not yet seen this URL,
    if url not in url2num:
        # THEN add it to both book-keeping data structures
        url2num[url] = len(url2num) # NOTE: numbering starts with zero
        num2url.append(url)
        # AND print it to the node list output file
        ofn.write("%d\t%s\n" % (url2num[url],url))
        # AND IF needed, THEN flush the output file
        if flushOutFiles:
            ofn.flush() 

# ----------------------------

def initOutFiles_downloadData_writeStdout_writeOutFiles(
        urlSetAll, urlListFetch, urlTimeOut, nMaxSimultReq, sleepTime, urlDomain, outFileNodes, outFileLinks, flushStdout, flushOutFiles ):

    # numerical ID to full URL
    num2url = []
    # full URL to numerical ID
    url2num = {}
    
    # --- open output files, print their headers ---
    # ofn: node (page) number to full URL mapping
    # ofl: list of directed links (hyperlinks) with numbered nodes (URLs)
    with open(outFileNodes,"w") as ofn: 
        with open(outFileLinks,"w") as ofl:
            ofn.write("# Numerical ID of the page\n")
            ofn.write("#\tFull URL of the page\n")
            ofn.write("\n")
            ofl.write("# Numerical ID of source node (page, URL)\n")
            ofl.write("#\tNumerical ID of target node\n")
            ofl.write("\n")
            # IF needed, THEN flush outfiles
            if flushOutFiles:
                ofn.flush() 
                ofl.flush()

                # --- proceed as long as we have at least one more URL to fetch ---
                while len(urlListFetch) > 0:
                    # list of URLs to be fetched with the current request
                    urlListFetch_now = []
                    # pop URLs from the beginning of the list:
                    # either the max. allowed number (nMaxSimultReq) or the max. needed number (to be fetched)
                    while len(urlListFetch_now) < nMaxSimultReq and len(urlListFetch) > 0:
                        url_now = urlListFetch.pop(0)
                        urlListFetch_now.append(url_now)
                    # current time: number of seconds since the epoch, formatted Greenwich Mean (GM) time
                    sec_since_epoch = str(calendar.timegm(time.gmtime()))
                    gm_time = time.strftime("%a %d %b %Y %H:%M:%S",time.gmtime())

                    # try to download
                    try:
                        responseList = grequests.map( (grequests.get(url,timeout=urlTimeOut) for url in urlListFetch_now) )
                    # handle exceptions
                    except:
                        # get info about the exception, i.e., the error
                        error = sys.exc_info()
                        # log the error
                        sys.stderr.write("Error at time %s while fetching these URLs:\n%s\nError details:\n%s\n" %
                                   (gm_time, '\n'.join(map(lambda _: '  '+_, urlListFetch_now)), '\n'.join(map(lambda _:'  '+str(_), error) )))
                        sys.stderr.flush()
                        # append again the currently failed URL list (maybe only one failed) to the end of the "fetch list"
                        urlListFetch.extend(urlListFetch_now)
                    # IF the download was successful,
                    else:
                        # NOTE: 'url_source' is the URL of the current page, from which we are checking outgoing links
                        #       'url_target' is the URL of a page to which there is a hyperlink from the current page
                        # THEN process the downloaded data and the status flag
                        for response in responseList:
                            # IF the current respone is undefined, THEN skip it
                            if response:
                                # set the source URL
                                # NOTE: response.url is the actually downloaded URL,
                                #       thus, -- due to redirection -- it can be different from the original URL
                                # NOTE: we assume that the redirection stays within the requested domain
                                url_source = response.url
                                # remove closing / character
                                url_source = re.sub(r'\/$', '', url_source)

                                # --- add the source URL to the book-keeping of URLs ---
                                numberThisUrlIfNew(url_source,url2num,num2url,flushOutFiles,ofn)
                                
                                # ---- print to stdout ----
                                # print status code
                                sys.stdout.write("%d" % response.status_code)
                                # print page length
                                # NOTE: IF the status code is 200, THEN print the length of the downloaded page
                                #       ELSE print a "-" sign, meaning that we have no length data for the current URL
                                if 200 == int(response.status_code):
                                    sys.stdout.write("\t%d" % len(response.text.encode('utf-8')))
                                else:
                                    sys.stdout.write("\t-")
                                # print source url
                                sys.stdout.write("\t%s" % url_source)
                                # print line break to close the current line of stdout
                                sys.stdout.write("\n")
                                # IF needed, THEN flush stdout
                                if flushStdout:
                                    sys.stdout.flush()

                                # ---- extract outgoing hyperlinks ----
                                # analyze the downloaded html source code: extract all full URLs which the current html page links to
                                # targetUrlSet: the set of all target URLs in standardized format
                                targetUrlSet = set()
                                # loop through the list of unformatted target URLs
                                # convert all target URLs to standard format, keep only target URLs from the requested domain
                                for url_target_loop_var in re.findall(r'href=(\".+?\"|\'.+?\')', response.text.encode('utf-8'), flags=re.IGNORECASE):
                                    # the variable 'url_target' will be changed (formatted)
                                    url_target = url_target_loop_var
                                    # remove leading and trailing quotes
                                    url_target = re.sub(r'^(\"|\')|(\"|\')$', '', url_target)
                                    # remove html anchors (pointers within the html page)
                                    url_target = re.sub(r'\#.+?$', '', url_target)
                                    
                                    # IF the target URL contains a ? (meaning a dynamic request), THEN blank the target URL entirely
                                    url_target = re.sub(r'^.*?\?.*?$', '', url_target)
                                    # discard common image formats, and also js, json, xml, cfm, pdf, css, gz, zip, ico
                                    url_target = re.sub(r'^.*?\.(jpg|gif|png|jpeg|tiff?|js|json|xml|cfm|pdf|css|gz|zip|ico)$', '', url_target, flags=re.IGNORECASE)
                                    # discard URL ending in "feed"
                                    url_target = re.sub(r'^.*?\/feed$', '', url_target)
                                    # discard URL containing @
                                    url_target = re.sub(r'^.*?\@.*?$', '', url_target)
                                    # discard URL starting with 'javascript'
                                    url_target = re.sub(r'^javascript.*?$', '', url_target)
                                    
                                    # convert the target URL to a full valid target URL
                                    # OR discard it by blanking it out entirely
                                    # NOTE: the source URL and the site of the source URL is needed
                                    #       when the target URL is a relative URL compared to either the source URL or the source URL site
                                    url_target = rawUrl2fullUrl(url_target, url_source, fullUrl2siteUrl(url_source), fullUrl2urlDir(url_source))
                                    
                                    # IF the target URL is non-empty
                                    if not re.match(r'^\s*$',url_target):
                                        # THEN determine the site of the target URL
                                        url_target_site = fullUrl2siteUrl(url_target)
                                        # AND IF the site of the target URL is the requested domain
                                        # i.e., the end of the string 'url_target_site' is the string 'urlDomain' (the requested domain)
                                        if url_target_site[-len(urlDomain):] == urlDomain:
                                            # THEN add the target URL to the set of target URLs for the current source URL
                                            targetUrlSet.add(url_target)
                                            
                                # ---     add all target URLs to the book-keeping of URLs ---
                                # --- AND print the source URL -> target URL directed links to the link list output file ---
                                # --- AND add target URLs to the 'all' set and the 'fetch' list ---
                                for url_target in targetUrlSet:
                                    # add target URL to the book-keeping
                                    numberThisUrlIfNew(url_target,url2num,num2url,flushOutFiles,ofn)
                                    # print the source URL -> target URL directed link
                                    ofl.write("%d\t%d\n" % (url2num[url_source],url2num[url_target]))
                                    # IF needed, THEN flush the output file
                                    if flushOutFiles:
                                        ofl.flush()
                                        # IF the current target URL is not yet in the set of all URLs,
                                        if url_target not in urlSetAll:
                                            # THEN append it to the list of URLs that we still need to fetch
                                            urlListFetch.append(url_target)
                                            # AND add it to the set of all URLs
                                            urlSetAll.add(url_target)
                                    
                        # IF we need to wait,
                        if sleepTime > 0:
                            # THEN wait for the requested number of seconds
                            time.sleep(sleepTime)

# ========== main ==========

# the set of all URLs (pages, nodes) that we have downloaded so far
_urlSetAll = set()

# the list of URLs that we would like to download
_urlListFetch = []

# initialize data structures and stdout
initDat_initStdout( _firstUrl, _urlSetAll, _urlListFetch, _flushStdout )

# initialize the output files, download data, write stdout and output files
initOutFiles_downloadData_writeStdout_writeOutFiles(
    _urlSetAll, _urlListFetch, _urlTimeOut, _nMaxSimultReq, _sleepTime, _urlDomain, _outFileNodes, _outFileLinks, _flushStdout, _flushOutFiles )
