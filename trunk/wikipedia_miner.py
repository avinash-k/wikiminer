'''
Copyright Yongqian Li
This code is licensed under the GNU General Public License v3 (GPLv3)
'''

from __future__ import with_statement


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

class WikiDumpHandler:
    def __init__(self, pass_num):
        self.pass_num = pass_num
        self.num_articles = 0
        self.num_links = 0
    
    def startDump(self):
        #delete previous data

        if self.pass_num == 0:
            models.clear()
        elif self.pass_num == 1:
            pass

    
    def endDump(self):
        models.sync()



    def handleArticle(self, current_page):
        #print 'ARTICLE:', repr(current_page.title)

        if self.pass_num == 0:
            models.save_article(models.Article(id=current_page['id'], title=current_page['title']))
        elif self.pass_num == 1:
            aid = models.title_to_aid(current_page['title'])            
            wikitext__ = utils.unescape_html(current_page['text'])
            
            redirect = wikitext.parse_redirect(wikitext__)
            if redirect:
                redirect_dest_title, dest_frag, redirect_label = redirect
                article = models.load_article(aid)
                article.redirect = models.Redirect(models.title_to_aid(redirect_dest_title), dest_frag, redirect_label)
                models.save_article(article)
            else:
                #do not allow/parse links if there is a redirect. If there is a redirect, links are meaningless. besides, parse_links does not work properly with redirects.
                for link_dest_title, dest_frag, link_label, snippet in wikitext.parse_links(wikitext__):

                    lid = self.num_links + 1 #0 is an invalid key
                    assert lid not in models.lid_to_link_db
                    
                    models.save_link(models.Link(lid, aid, models.title_to_aid(link_dest_title), dest_frag, link_label, snippet))
                    self.num_links += 1

        self.num_articles += 1

        if self.num_articles % 1000 == 0:
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



def build_graph(session):
    models.vertices_table.drop(bind=models.engine)
    models.vertices_table.create(bind=models.engine)
    models.edges_table.drop(bind=models.engine)
    models.edges_table.create(bind=models.engine)
    #create a vertex for every article
    for article in session.query(models.Article):
        #follow all redirects, if any
        while True:
            try:
                redirect = session.query(models.Redirect).filter_by(src=article).one()
                article = redirect.dest
            except sqlalchemy.orm.exc.NoResultFound: 
                break
        vertex = models.Vertex()
        session.add(vertex)
        article.vertex = vertex
        session.commit()

    for link in session.query(models.Link):
        if link.dest: #valid edge
            edge = models.Edge(link.src.vertex, link.dest.vertex)
            session.add(edge)
            session.commit()
    


def do_parse_dump(dump_filename):
    print 'begin do_parse_dump'
    import datetime
    print datetime.datetime.now()

    
    import bz2

    dump_file = bz2.BZ2File(dump_filename, 'rb')
    #parse_dump(dump_file, 0)
    dump_file.close()
    print 'finished first pass parsing'
    
    dump_file = bz2.BZ2File(dump_filename, 'rb')
    parse_dump(dump_file, 1)
    dump_file.close()
    print 'finished second pass parsing'


    print 'done do_parse_dump'
    print datetime.datetime.now()


def do_build_graph():
    print 'begin do_parse_dump'
    print datetime.datetime.now()


    session = models.Session()
    build_graph(session)
    

    print 'done do_parse_dump'
    print datetime.datetime.now()





def main():
    import os.path
    current_dir = os.path.dirname(os.path.abspath(__file__))

    dump_filename = 'enwiki-20080724-pages-articles.xml.bz2'
    dump_filename = os.path.join(current_dir, dump_filename)

    do_parse_dump(dump_filename)    
    do_build_graph()




if __name__ == '__main__':
    main()












'''
except BaseException, e:
    print 'ERROR::::::::::::::::::::::::::::::::::::::::::::::::::::::::'
    print repr(self.current_page.title)
    print repr(chars)
    print e
    print repr(e)                    
'''