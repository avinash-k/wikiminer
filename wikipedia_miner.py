'''
Copyright Yongqian Li
This code is licensed under the GNU General Public License v3 (GPLv3)
'''

#from __future__ import print_function


import sys
sys.path.append('/cs/liy12/python-packages/')
sys.path.append('/home/yongqli/sources/sqlalchemy/lib/')

import xml.sax
import xml.sax.handler
import time
import datetime


import utils
import models
import wikitext


parse_log = open('parse.log', 'a')

class WikiDumpHandler:
    def __init__(self, pass_num):
        self.pass_num = pass_num
        self.num_articles = 0
        self.num_links = 0
    
    def startDump(self):
        #delete previous data

        if self.pass_num == 0:
            #models.clear()
            pass
        elif self.pass_num == 1:
            pass

    
    def endDump(self):
        models.sync()



    def handleArticle(self, current_page):
        #print('ARTICLE:', repr(current_page.title))

        if self.pass_num == 0:
            models.articles.save(models.Article(id=current_page['id'], title=current_page['title'], revision_id=current_page['revision_id']))
        elif self.pass_num == 1:
            aid = models.articles.resolve_title(current_page['title'])            
            wikitext__ = utils.unescape_html(current_page['text'])
            
            redirect = wikitext.parse_redirect(wikitext__)
            if redirect:
                redirect_dest_title, dest_frag, redirect_label = redirect
                article = models.articles[aid]
                if redirect_dest_title in models.articles:
                    dest_id = models.articles.resolve_title(redirect_dest_title)
                else:
                    dest_id = None
                    #print('Broken REDIRECT FROM ', repr(current_page['title']), ' TO ', repr(redirect), file=parse_log) 
                    #print(repr(redirect_dest_title), file=parse_log)
                    print >>parse_log, repr(redirect_dest_title)
                article.redirect = models.Redirect(dest_id, dest_frag, redirect_label)
                models.articles.save(article)
            else:
                #do not parse/allow links if there is a redirect. If there is a redirect, links are meaningless. besides, parse_links does not work properly with redirects.
                for link_dest_title, dest_frag, link_label, snippet in wikitext.parse_links(wikitext__):
                    lid = self.num_links + 1 #0 is an invalid key
                    #assert lid not in models.links
                    if link_dest_title in models.articles:
                        dest_id = models.articles.resolve_title(link_dest_title)
                    else:
                        dest_id = None
                        #print('Broken LINK FROM ', repr(current_page['title']), ' TO ', repr((link_dest_title, dest_frag, link_label)), file=parse_log) 
                        #print(repr(link_dest_title), file=parse_log)
                        print >>parse_log, repr(link_dest_title)
                    models.links.save(models.Link(src_id=aid, dest_id=dest_id, dest_fragment=dest_frag,
                        label=link_label, snippet=snippet, id=lid))
                    self.num_links += 1
                    '''
                    if lid > len(models.links):
                        ...
                    else:
                        self.num_links += 1 #skip
                    '''
                        
        self.num_articles += 1

        if self.num_articles % 1000 == 0:
            #print(self.num_articles, self.num_links)
            print self.num_articles, self.num_links

class WikiDumpContentHandler(xml.sax.handler.ContentHandler):
    #A finite state machine
    def __init__(self, dump_handler):
        self.open_tags = []
        self.char_buffer = []
        self.current_page = None
        self.dump_handler = dump_handler

    def startDocument(self):
        assert self.open_tags == []
        self.dump_handler.startDump()

    def endDocument(self):
        assert self.open_tags == [] 
        self.dump_handler.endDump()

    def startElement(self, name, attrs):
        self.open_tags.append(name)
        self.char_buffer = []
        if name == 'page':
            assert self.current_page is None
            self.current_page = {}

    def endElement(self, name):
        assert self.open_tags[-1] == name
        self.open_tags.pop()
        chars = ''.join(self.char_buffer)

        if name == 'id':
            if self.open_tags[-1] == 'page':
                self.current_page['id'] = int(chars)
            elif self.open_tags[-1] == 'revision':
                self.current_page['revision_id'] = int(chars)
        elif name == 'title':
            self.current_page['title'] = chars                
        elif name == 'text':
            self.current_page['text'] = chars
        elif name == 'page':
            self.dump_handler.handleArticle(self.current_page)
            self.current_page = None


    def characters(self, content):
        self.char_buffer.append(content)




def parse_dump(dump_file, pass_num):
    xml.sax.parse(dump_file, WikiDumpContentHandler(WikiDumpHandler(pass_num)))



def resolve_redirects(article):
    aaa = article
    redirects_traversed = 0
    while article.redirect and redirects_traversed < 6:
        if article.redirect.dest_id is not None:
            article = models.articles[article.redirect.dest_id]
        else:
            return None #article redirects to non-existent article, so it itself is None
        redirects_traversed += 1
        
    if not redirects_traversed < 6:
       print>>parse_log, 'Cycle in redirects detected', repr(aaa)  
    return article
'''
<!--NOTE: [[Courts of love]] and [[Court of love]] redirect here as #REDIRECT[[Courtly love#Court of love]] - Please keep this section name in sync.-->
'''

def build_graph():
    adj_list = {}
    num_articles = 0    
    for article in models.articles:
        article = resolve_redirects(article)
        if article: #'article' does not redirect to non-existent article
            for dest_id, lid in models.links.query(src_id=article.id):
                if dest_id is not None:
                    dest = resolve_redirects(models.articles[dest_id])
                    if dest is None: #broken redirect
                        print>>parse_log, 'Broken link redirect:', 'title:', repr(article.title), 'label:', repr(models.links[lid].label), 'link.dest_id:', repr(models.links[lid].dest_id), 'target title:', repr(models.articles[models.links[lid].dest_id].title)
                        print>>parse_log, repr(models.articles[dest_id].title)
                    else:
                        if article.id not in adj_list:
                            adj_list[article.id] = []
                        adj_list[article.id].append(dest.id)

        num_articles += 1
        if num_articles % 5000 == 0:
            print num_articles

    for k, v in adj_list.iteritems():
        models.graph[k] = set(v) # get rid of duplicates
                    
    


def do_parse_dump(dump_filename):
    #print('begin do_parse_dump')
    import datetime
    #print(datetime.datetime.now())

    
    import bz2

    dump_file = bz2.BZ2File(dump_filename, 'rb')
    parse_dump(dump_file, 0)
    dump_file.close()
    #print('finished first pass parsing')
    #print(datetime.datetime.now())
    
    dump_file = bz2.BZ2File(dump_filename, 'rb')
    parse_dump(dump_file, 1)
    dump_file.close()
    #print('finished second pass parsing')
    #print(datetime.datetime.now())


    #print('done do_parse_dump')
    #print(datetime.datetime.now())


def do_build_graph():
    print('begin do_build_graph')
    print(datetime.datetime.now())


    build_graph()
    

    print('done do_build_graph')
    print(datetime.datetime.now())





def main():
    import os.path
    current_dir = os.path.dirname(os.path.abspath(__file__))

    #dump_filename = 'Z:\\wikiminer\\enwiki-20080724-pages-articles.xml.bz2'
    dump_filename = 'enwiki-20080724-pages-articles.xml.bz2'
    dump_filename = os.path.join(current_dir, dump_filename)

    do_parse_dump(dump_filename)    
    do_build_graph()




if __name__ == '__main__':
    main()


'''
metis wikiminer $ cat enwiki-20081008-pages-articles.xml.bz2 | bzip2 -d | wc --bytes
19519819486
metis wikiminer $

'''











'''
Cerin Amroth



#redirect [[Minor places in Middle-earth#Cerin Amroth]] {{MER to list entry}}
[[Category:Middle-earth hills]]



'''