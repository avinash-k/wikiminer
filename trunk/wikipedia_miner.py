'''
Copyright Yongqian Li
This code is licensed under the GNU General Public License v3 (GPLv3)
'''


from __future__ import with_statement
import sqlalchemy
import sqlalchemy.orm


class Vertex(object):
    def __init__(id):
        self.id = id

    
class Edge(object):
    def __init__(self, src, dest):
        self.src = src
        self.dest = dest


class Redirect(object):
    def __init__(self, src, dest):
        self.src = src
        self.dest = dest
    
    def __repr__(self):
        return "Redirect(title='%s', article=A('%s'))" % (self.title, self.article.title)


class Article(object):
    def __init__(self, title=None, revision_id=None, vertex=None):
        super(Article, self).__init__()
        self.title = title
        self.revision_id = revision_id
        self.vertex = vertex

    def parse_redirect(self, session):
        import re
        sre_match = re.search('#REDIRECT\s*:?\s*\[\[\s*:?\s*(.*?)(?:\|(.*))?\]\]', self.text, re.IGNORECASE)
        if sre_match:
            redirect = sre_match.group(1)
            redirect_title = sre_match.group(2) # not supported by wikipedia
            if redirect_title:
                print 'title', self.title, '|', 'redirect', redirect, '|', 'redirect title', redirect_title
            '''
            print repr(self.title)
            print '---> %s' % repr(redirect)
            '''

        '''
        if 'REDIRECT' in self.text and not sre_match:
            print repr(self.title)
            print 'ERROR????????????????????????????????????????????????????????'
            print repr(self.text[max(0, self.text.index('REDIRECT')-16):self.text.index('REDIRECT') + 32])
        '''
        
        session.add(self)
        session.commit()    
        
    def parse_links(self, session):
        import re
        sre_match = re.search('\[\[\s*:?\s*(.*?)(?:\|(.*))?\]\]', self.text, re.IGNORECASE)
        if sre_match:
            link_dest_title = sre_match.group(1)
            link_label = sre_match.group(2) # not supported by wikipedia
            if link_label:
                print 'from:', self.title, 'to', link_dest_title, 'label', link_label
            try:
                link_dest = session.query(Article).filter_by(title=link_dest_title).one()
            except sqlalchemy.orm.NoResultError:
                link_dest = None
            session.add(Link(self, link_label, link_dest))
            session.commit()    


    def __repr__(self):
        return "Article(title='%s', revision_id='%s')'" % (self.title, self.revision_id)
        

class Link(object):
    def __init__(self, src, label, dest, edge):
        self.src = src
        self.label = label
        self.dest = dest
        self.edge = edge


import os.path
current_dir = os.path.dirname(os.path.abspath(__file__))
engine = sqlalchemy.create_engine('sqlite:///%s' % os.path.join(current_dir, 'wikipedia.sqlite'))

metadata = sqlalchemy.MetaData()
vertices_table = sqlalchemy.Table('vertices', metadata,
                    sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
                )
edges_table = sqlalchemy.Table('edges', metadata,
                    sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
                    sqlalchemy.Column('src', sqlalchemy.Integer, sqlalchemy.ForeignKey(vertices_table.c.id), nullable=False),
                    sqlalchemy.Column('dest', sqlalchemy.Integer, sqlalchemy.ForeignKey(vertices_table.c.id), nullable=False),
                ) 
articles_table = sqlalchemy.Table('articles', metadata,
                    sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
                    sqlalchemy.Column('title', sqlalchemy.String(1024), unique=True),
                    sqlalchemy.Column('revision_id', sqlalchemy.Integer),
                    sqlalchemy.Column('vertex', sqlalchemy.Integer, sqlalchemy.ForeignKey(vertices_table.c.id), nullable=True),
                )
redirect_table = sqlalchemy.Table('redirects', metadata,
                    sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
                    sqlalchemy.Column('src', sqlalchemy.Integer, sqlalchemy.ForeignKey(articles_table.c.id), nullable=False, unique=True),
                    sqlalchemy.Column('dest', sqlalchemy.Integer, sqlalchemy.ForeignKey(articles_table.c.id), nullable=True),
                )       
link_table = sqlalchemy.Table('links', metadata,
                    sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
                    sqlalchemy.Column('src', sqlalchemy.Integer, sqlalchemy.ForeignKey(articles_table.c.id), nullable=False),
                    sqlalchemy.Column('label', sqlalchemy.String(10240)),                    
                    sqlalchemy.Column('dest', sqlalchemy.Integer, sqlalchemy.ForeignKey(articles_table.c.id), nullable=True),
                    sqlalchemy.Column('edge', sqlalchemy.Integer, sqlalchemy.ForeignKey(edges_table.c.id), nullable=True),
                )                   


#metadata.create_all(engine)


sqlalchemy.orm.mapper(Article, articles_table) 
sqlalchemy.orm.mapper(Redirect, redirect_table) 
sqlalchemy.orm.mapper(Link, link_table)


Session = sqlalchemy.orm.sessionmaker(bind=engine)




def parse_dump(filename):
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
                self.current_page.text = chars
            elif name == 'page':
                if self.pass_num == 0:
                    self.current_page.parse_redirect(self.session)
                elif self.pass_num == 1:
                    self.current_page.parse_links(self.session)
                self.current_page = None



        def characters(self, content):
            self.char_buffer.append(content)
            
        
    dump_file = bz2.BZ2File(filename, 'rb')
    session = Session()
    #delete all previous data
    metadata.drop_all(engine)
    metadata.create_all(engine)
        
    xml.sax.parse(dump_file, WikiDumpContentHandler(0, session))
    xml.sax.parse(dump_file, WikiDumpContentHandler(1, session))
    session.commit()    



def main():
    parse_dump(os.path.join(current_dir, 'enwiki-20080724-pages-articles.xml.bz2'))
    

if __name__ == '__main__':
    import datetime

    print datetime.datetime.now()
    main()
    print datetime.datetime.now()
