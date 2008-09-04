'''
Copyright Yongqian Li
This code is licensed under the GNU General Public License v3 (GPLv3)
'''


from __future__ import with_statement
import re
import sqlalchemy
import sqlalchemy.orm
import sqlalchemy.orm.exc


class Vertex(object):
    def __init__(self, id):
        self.id = id

    
class Edge(object):
    def __init__(self, src, dest):
        self.src = src
        self.dest = dest


class Redirect(object):
    def __init__(self, src, dest=None, dest_fragment=None):
        self.src = src
        self.dest = dest
        self.dest_fragment
    
    def __repr__(self):
        return "Redirect(title='%s', article=A('%s'))" % (self.title, self.article.title)


class Article(object):
    def __init__(self, title=None, revision_id=None, vertex=None):
        super(Article, self).__init__()
        self.title = title
        self.revision_id = revision_id
        self.vertex = vertex

    
    @staticmethod
    def parse_links(text):
        re_pattern = '''\[\[\s*(?::\s*(?P<colon_escaped_title>.*?)|\s*(?P<unescaped_title>[^:]*?))(?:\|(?P<link_label>.*?))?\]\](?P<link_label2>[^ "<\]]*)'''#<nowiki>d</nowiki>
        for sre_match in re.finditer(re_pattern, text, re.IGNORECASE):
            link_dest_title = sre_match.group('colon_escaped_title') or sre_match.group('unescaped_title')
            frag_match = re.match('(.*)#(.*)$', link_dest_title)
            link_dest_title, dest_frag = frag_match.group(1), frag_match.group(2)
                
            link_label = sre_match.group('link_label')
            link_label2 = sre_match.group('link_label2')
            if link_label2:
                link_label = (link_label or '') + link_label2
            yield link_label, link_dest_title, dest_frag

        #for sre_match in re.finditer('\[\[\s*:?\s*(.*?)(?:\|(.*?))?\]\]([^ "]*?)', self.text, re.IGNORECASE):


    @staticmethod
    def parse_redirect(text):
        sre_match = re.search('#REDIRECT\s*:?\s*\[\[\s*:?\s*(.*?)(?:\|(.*?))?\]\]', text, re.IGNORECASE)
        if sre_match:
            redirect_title = sre_match.group(1)
            frag_match = re.match('(.*)#(.*)$', redirect_title)
            redirect_title, dest_frag = frag_match.group(1), frag_match.group(2)            
            redirect_label = sre_match.group(2) # not supported by wikipedia
            return redirect_label, redirect_title, dest_frag
        
        
        
    def parse_text(self, session):
        redirect = self.parse_redirect(self.text)
        if redirect:
            redirect_label, redirect_title, dest_frag = redirect
            if redirect_label:
                print 'title:', repr(self.title), 'redirect_title:', repr(redirect_title), 'redirect_label:', repr(redirect_label)            
            try:
                redirect_dest = session.query(Article).filter_by(title=redirect_title).one()
            except sqlalchemy.orm.exc.NoResultFound:
                redirect_dest = None            
            session.add(Redirect(self, redirect_dest)) 

        for link_label, link_dest_title, dest_frag in self.parse_links(self.text):
            print 'LINK from:', repr(self.title), 'to', repr(link_dest_title + '#' + dest_frag), 'label', repr(link_label)
            try:
                link_dest = session.query(Article).filter_by(title=link_dest_title).one()
            except sqlalchemy.orm.exc.NoResultFound:
                link_dest = None
            print link_dest
            session.add(Link(self, link_label, link_dest, dest_frag))

        session.commit()    




        '''
        if 'REDIRECT' in self.text and not sre_match:
            print repr(self.title)
            print 'ERROR????????????????????????????????????????????????????????'
            print repr(self.text[max(0, self.text.index('REDIRECT')-16):self.text.index('REDIRECT') + 32])
        '''

    def __repr__(self):
        return "Article(title='%s', revision_id='%s')" % (self.title, self.revision_id)
        

class Link(object):
    def __init__(self, src, label=None, dest=None, dest_fragment=None, edge=None):
        self.src = src
        self.label = label
        self.dest = dest
        self.dest_fragment = dest_fragment
        self.edge = edge


import os.path
current_dir = 'Z:'#os.path.dirname(os.path.abspath(__file__))
engine = sqlalchemy.create_engine('sqlite:///%s' % os.path.join(current_dir, 'wikipedia.sqlite'))

metadata = sqlalchemy.MetaData()
vertices_table = sqlalchemy.Table('vertices', metadata,
                    sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
                )
edges_table = sqlalchemy.Table('edges', metadata,
                    sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
                    sqlalchemy.Column('src_id', sqlalchemy.Integer, sqlalchemy.ForeignKey(vertices_table.c.id), nullable=False, index=True),
                    sqlalchemy.Column('dest_id', sqlalchemy.Integer, sqlalchemy.ForeignKey(vertices_table.c.id), nullable=False, index=True),
                ) 
articles_table = sqlalchemy.Table('articles', metadata,
                    sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
                    sqlalchemy.Column('title', sqlalchemy.String(1024), unique=True, index=True),
                    sqlalchemy.Column('revision_id', sqlalchemy.Integer),
                    sqlalchemy.Column('vertex_id', sqlalchemy.Integer, sqlalchemy.ForeignKey(vertices_table.c.id), nullable=True, index=True),
                )
redirects_table = sqlalchemy.Table('redirects', metadata,
                    sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
                    sqlalchemy.Column('src_id', sqlalchemy.Integer, sqlalchemy.ForeignKey(articles_table.c.id), nullable=False, unique=True, index=True),
                    sqlalchemy.Column('dest_id', sqlalchemy.Integer, sqlalchemy.ForeignKey(articles_table.c.id), nullable=True, index=True),
                    sqlalchemy.Column('dest_fragment', sqlalchemy.String(10240), nullable=True),
                )       
links_table = sqlalchemy.Table('links', metadata,
                    sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
                    sqlalchemy.Column('src_id', sqlalchemy.Integer, sqlalchemy.ForeignKey(articles_table.c.id), nullable=False, index=True),
                    sqlalchemy.Column('label', sqlalchemy.String(10240), nullable=True),
                    sqlalchemy.Column('excerpt', sqlalchemy.String(10240), nullable=True),
                    sqlalchemy.Column('dest_id', sqlalchemy.Integer, sqlalchemy.ForeignKey(articles_table.c.id), nullable=True, index=True),
                    sqlalchemy.Column('dest_fragment', sqlalchemy.String(10240), nullable=True),
                    sqlalchemy.Column('edge_id', sqlalchemy.Integer, sqlalchemy.ForeignKey(edges_table.c.id), nullable=True, index=True),
                )                   


metadata.create_all(engine)


sqlalchemy.orm.mapper(Vertex, vertices_table)

sqlalchemy.orm.mapper(Edge, edges_table, properties={
    'src': sqlalchemy.orm.relation(Vertex, primaryjoin=edges_table.c.src_id==vertices_table.c.id),
    'dest': sqlalchemy.orm.relation(Vertex, primaryjoin=edges_table.c.dest_id==vertices_table.c.id),
}) 

sqlalchemy.orm.mapper(Article, articles_table, properties={
    'vertex': sqlalchemy.orm.relation(Vertex),
})

sqlalchemy.orm.mapper(Redirect, redirects_table, properties={
    'src': sqlalchemy.orm.relation(Article, primaryjoin=redirects_table.c.src_id==articles_table.c.id),
    'dest': sqlalchemy.orm.relation(Article, primaryjoin=redirects_table.c.dest_id==articles_table.c.id),
}) 

sqlalchemy.orm.mapper(Link, links_table, properties={
    'src': sqlalchemy.orm.relation(Article, primaryjoin=links_table.c.src_id==articles_table.c.id),
    'dest': sqlalchemy.orm.relation(Article, primaryjoin=links_table.c.dest_id==articles_table.c.id),
    'edge': sqlalchemy.orm.relation(Edge),
})


Session = sqlalchemy.orm.sessionmaker(bind=engine)


def unescape_html(s):
    import re
    import htmlentitydefs
    def decode_entity(match):
        entity = match.group(1)
        if entity.startswith('#x'):
            return unichr(int(entity[2:], 16))
        if entity.startswith('#'):
            return unichr(int(entity[1:]))
        try:
            return unichr(htmlentitydefs.name2codepoint[entity])
        except KeyError:
            return match.group(0)
    s = s.replace('&apos;', '\'')
    return re.sub(r'&(#\d+|#x[\da-fA-F]+|[\w.:-]+);', decode_entity, s)
    #                (dec |hex          |named   )
    #is ; required? if not, use r'&(#\d+|#x[\da-fA-F]+|[\w.:-]+);?'
    

def parse_dump(filename, session):
    import bz2
    import xml.sax
    import xml.sax.handler
    
    class WikiDumpContentHandler(xml.sax.handler.ContentHandler):
        #A finite state machine
        def __init__(self, pass_num, session):
            self.open_tags = []
            self.char_buffer = []
            self.pass_num = pass_num
            self.session = session

        def startDocument(self):
            assert self.open_tags == []
            pass
        
        def endDocument(self):
            assert self.open_tags == [] 
            pass
        
        def startElement(self, name, attrs):
            self.open_tags.append(name)
            self.char_buffer = []
            if name == 'page':
                self.current_page = Article()
        
        def endElement(self, name):
            assert self.open_tags[-1] == name
            self.open_tags.pop()
            chars = ''.join(self.char_buffer)
            
            if name == 'id':
                if self.open_tags[-1] == 'page':
                    self.current_page.id = int(chars)
                elif self.open_tags[-1] == 'revision':
                    self.current_page.revision_id = int(chars)
            elif name == 'title':
                self.current_page.title = chars #chars.trim()                
            elif name == 'text':
                self.current_page.text = unescape_html(chars)
            elif name == 'page':
                if self.pass_num == 0:
                    session.add(self.current_page)
                    session.commit()
                elif self.pass_num == 1:
                    self.current_page.parse_text(self.session)
                self.current_page = None


        def characters(self, content):
            self.char_buffer.append(content)
            
            
    
    dump_file = bz2.BZ2File(filename, 'rb')
    #delete previous data
    articles_table.drop(bind=engine)
    articles_table.create(bind=engine)
    xml.sax.parse(dump_file, WikiDumpContentHandler(0, session))
    dump_file.close()
    print 'finished first pass parsing'
    
    
    dump_file = bz2.BZ2File(filename, 'rb')
    #delete previous data
    redirects_table.drop(bind=engine)
    redirects_table.create(bind=engine)
    links_table.drop(bind=engine)
    links_table.create(bind=engine)
    xml.sax.parse(dump_file, WikiDumpContentHandler(1, session))
    dump_file.close()
    print 'finished second pass parsing'


    session.commit()    


def build_graph(session):
    vertices_table.drop(bind=engine)
    vertices_table.create(bind=engine)
    edges_table.drop(bind=engine)
    edges_table.create(bind=engine)
    #create a vertex for every article
    for article in session.query(Article).all():
        #follow all redirects, if any
        while True:
            try:
                redirect = session.query(Redirect).filter_by(src=article).one()
                article = redirect.dest
            except sqlalchemy.orm.exc.NoResultFound: 
                break
        vertex = Vertex()
        session.add(vertex)
        article.vertex = vertex
        session.commit()

    for link in session.query(Link).all():
        edge = Edge(link.src.vertex, link.dest.vertex)
        session.add(edge)
        session.commit()
    

def main():
    session = Session()
    parse_dump(os.path.join(current_dir, 'enwiki-20080724-pages-articles.xml.bz2'), session)
    

if __name__ == '__main__':
    import datetime

    print datetime.datetime.now()
    #main()
    print datetime.datetime.now()
