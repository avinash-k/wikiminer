'''
Copyright Yongqian Li
Released under GPL v3
'''


class Redirect(object):
    def __init__(self, dest_id=None, dest_fragment=None, label=None):
        self.dest_id = dest_id
        self.dest_fragment = dest_fragment
        self.label = label
    
    def __repr__(self):
        #return 'Redirect(label={0}, article={1}))'.format(repr(self.label), repr(self.article))
        return "Redirect(label='%s', article=%s))" % (self.label, None if self.dest_id is None else repr(articles[self.dest_id]))


class Article(object):
    def __init__(self, id, title=None, revision_id=None, redirect=None):
        self.id = id
        self.title = title
        self.revision_id = revision_id
        self.redirect = redirect

    def __repr__(self):
        #return 'Article(id={0}, title={1}, revision_id={1})'.format(self.id, repr(self.title), self.revision_id)
        return "Article(id=%s, title=%s, revision_id=%s)" % (repr(self.id), repr(self.title), self.revision_id)
        

class Link(object):
    def __init__(self, src_id, dest_id=None, dest_fragment=None, label=None, snippet=None, id=None):
        self.src_id = src_id
        self.dest_id = dest_id
        self.dest_fragment = dest_fragment
        self.label = label
        self.snippet = snippet
        self.id = id










import os.path
current_dir = os.path.dirname(os.path.abspath(__file__))










import bsddb.db
import pickle

'''
title (utf-8) -> article_id
article_id -> Article object
link_id -> link_object
link_index: article_id, article_id -> [link_id]
adj_list: article_id -> [article_id]
'''

def iter_db(db):
    cursor = db.cursor()
    try:
        data = cursor.first()
        while data:
            yield data
            data = cursor.next()
    finally:
        cursor.close()

def iter_range(db, key_prefix):
    cursor = db.cursor()
    try:
        data = cursor.set_range(key_prefix)
        while data and data[0].startswith(key_prefix):
            yield data
            data = cursor.next()
    finally:
        cursor.close()
            
db_env = bsddb.db.DBEnv()
db_env.set_cachesize(2, 0, 1)
#db_env.set_cachesize(0, 536870912, 1)
db_env.open('db', bsddb.db.DB_CREATE | bsddb.db.DB_INIT_MPOOL)
            
#not really a mapping. sort of both mapping and list
class Articles:
    def __init__(self):
        self.aid_to_article_db = bsddb.db.DB(db_env)
        self.aid_to_article_db.open(filename='articles.db', dbname='aid_to_article', dbtype=bsddb.db.DB_RECNO, flags=bsddb.db.DB_CREATE)
        self.title_to_aid_db = bsddb.db.DB(db_env)
        self.title_to_aid_db.open(filename='article_index.db', dbname='title_to_aid', dbtype=bsddb.db.DB_BTREE, flags=bsddb.db.DB_CREATE)

    def __len__(self):
        assert len(self.aid_to_article_db) == len(self.title_to_aid_db)
        return len(self.aid_to_article_db)
    
    def __getitem__(self, key):
        if isinstance(key, int):
            aid = key
            return pickle.loads(self.aid_to_article_db[aid])
        elif isinstance(key, basestring):
            return pickle.loads(self.aid_to_article_db[self.resolve_title(key)])
        else:
            raise TypeError('lookup key must be article id (int) or article title (str/unicode), %s found' % type(key))

    def __iter__(self):
        return (pickle.loads(v) for k, v in iter_db(self.aid_to_article_db))


    def __contains__(self, key):
        if isinstance(key, int):
            aid = key
            return self.aid_to_article_db.has_key(aid)
        elif isinstance(key, basestring):
            title = key
            k = title.encode('utf-8')
            return self.title_to_aid_db.has_key(k)
        else:
            raise TypeError('lookup key must be article id (int) or article title (str/unicode), %s found' % type(key))

    def resolve_title(self, title):
        k = title.encode('utf-8')
        return int(self.title_to_aid_db[k])

    def by_id(self):
        return (k for k, v in iter_db(self.aid_to_article_db))

    def by_title(self):
        return (unicode(k, 'utf-8') for k, v in iter_db(self.title_to_aid_db))

    def save(self, article):
        k = article.id
        self.aid_to_article_db[k] = pickle.dumps(article, pickle.HIGHEST_PROTOCOL)
        k = article.title.encode('utf-8')
        self.title_to_aid_db[k] = str(article.id)

    def sync(self):
        self.aid_to_article_db.sync()
        self.title_to_aid_db.sync()        
        
    def close(self):
        self.aid_to_article_db.close()
        self.title_to_aid_db.close()        

articles = Articles()        

class Links:
    def __init__(self):
        self.lid_to_link_db = bsddb.db.DB(db_env)
        self.lid_to_link_db.open(filename='links.db', dbname='lid_to_link', dbtype=bsddb.db.DB_RECNO, flags=bsddb.db.DB_CREATE)
        self.link_index_db = bsddb.db.DB(db_env)
        self.link_index_db.open(filename='link_index.db', dbname='link_index', dbtype=bsddb.db.DB_BTREE, flags=bsddb.db.DB_CREATE)

    def __len__(self):
        return len(self.lid_to_link_db)

    def __contains__(self, key):
        lid = key
        return self.lid_to_link_db.has_key(lid)

    def __getitem__(self, key):
        lid = key
        return pickle.loads(self.lid_to_link_db[lid])

    def __iter__(self):
        return (pickle.loads(v) for k, v in iter_db(self.lid_to_link_db))
    
    '''
    WARNING: because the indexes are converted to strings and then inserted into the db, which
             sorts them lexigraphically, THEY WILL BE IN THE WRONG ORDER! For example, [1, 11, 2].
             However, keys will common prefixes will be sorted together, so query code works.
    '''

    def query(self, src_id=None, dest_id=None):
        if src_id is not None and dest_id is not None:
            result = []
            #key = 'src_id: {src_id} dest_id: {dest_id} '.format(src_id=src_id, dest_id=dest_id)
            key = 'src_id: %s dest_id: %s ' % (src_id, dest_id)
            for k, v in iter_range(self.link_index_db, key):
                result.append(int(k[len(key):]))
            return result
        elif src_id is not None and dest_id is None:
            result = []
            #key = 'src_id: {src_id} dest_id: '.format(src_id=src_id)
            key = 'src_id: %s dest_id: ' % src_id
            for k, v in iter_range(self.link_index_db, key):
                dest_id_s, lid_s = k[len(key):].split()
                dest_id = int(dest_id_s) if dest_id_s != 'None' else None
                result.append((dest_id, int(lid_s)))
            return result
        elif src_id is None and dest_id is not None:
            result = []
            #key = 'dest_id: {dest_id} src_id: '.format(dest_id=dest_id) 
            key = 'dest_id: %s src_id: ' % dest_id 
            for k, v in iter_range(self.link_index_db, key):
                src_id_s, lid_s = k[len(key):].split()
                src_id = int(src_id_s) if src_id_s != 'None' else None
                result.append((src_id, int(lid_s)))
            return result
        else:
            result = []
            key = 'src_id: ' #some keys start with 'dest_id: ', those are dups
            for k, v in iter_range(self.link_index_db, key):
                src_id_s = k.split()[1]
                dest_id_s = k.split()[3]
                lid_s = k.split()[4]
                src_id = int(src_id_s) if src_id_s != 'None' else None
                dest_id = int(dest_id_s) if dest_id_s != 'None' else None
                result.append((src_id, dest_id, int(lid_s)))
            return result

    def save(self, link):
        if link.id is None:
            link.id = self.lid_to_link_db.append(None)
        key = link.id
        self.lid_to_link_db[key] = pickle.dumps(link, pickle.HIGHEST_PROTOCOL)
        #key = 'src_id: {src_id} dest_id: {dest_id} {link_id}'.format(src_id=link.src_id, dest_id=link.dest_id, link_id=link.id)
        key = 'src_id: %s dest_id: %s %s' % (link.src_id, link.dest_id, link.id)
        self.link_index_db[key] = None
        #key = 'dest_id: {dest_id} src_id: {src_id} {link_id}'.format(src_id=link.src_id, dest_id=link.dest_id, link_id=link.id)
        key = 'dest_id: %s src_id: %s %s' % (link.dest_id, link.src_id, link.id)
        self.link_index_db[key] = None

    def sync(self):
        self.lid_to_link_db.sync()
        self.link_index_db.sync()
        
    def close(self):
        self.lid_to_link_db.close()
        self.link_index_db.close()

links = Links()

class Graph:
    def __init__(self, reversed=False):
        self.adj_list_db = bsddb.db.DB(db_env)
        dbname = 'adj_list_db'
        if reversed:
            dbname += '_r'
        self.adj_list_db.open(filename='graph.db', dbname=dbname, dbtype=bsddb.db.DB_RECNO, flags=bsddb.db.DB_CREATE)

    def __getitem__(self, key):
        aid = key
        return [int(x) for x in self.adj_list_db[aid].split()]
    
    def __setitem__(self, key, value):
        aid = key
        v = ' '.join(str(x) for x in value)
        self.adj_list_db[aid] = v

    def __iter__(self):
        return (k for k, v in iter_db(self.adj_list_db))
    
    def sync(self):
        self.adj_list_db.sync()
    
    def close(self):
        self.adj_list_db.close()
        

graph = Graph(True)
graph_r = Graph()

def sync():
    articles.sync()
    links.sync()
    graph.sync()
    graph_r.sync()

def close():
    articles.close()
    links.close()
    graph.close()
    graph_r.close()
    db_env.close()
    

def test():
    assert list(articles) == []
    assert list(links) == []
    
    articles.save(Article(5, '5'))
    articles.save(Article(6, '5'))
    articles.save(Article(6, '6'))
    articles.save(Article(23, '23'))
    
    assert list(articles.by_id()) == [5, 6, 23]
    assert list(articles.by_title()) == ['23', '5', '6']
    assert sorted(a.id for a in articles) == list(articles.by_id())
    
    
    links.save(Link(4)) #1
    links.save(Link(4, 5)) #2
    links.save(Link(3, 5)) #3
    links.save(Link(2)) #4
    
    assert links.query(4) == [(5, 2), (None, 1)]
    assert links.query(dest_id=5) == [(3, 3), (4, 2)]
    assert links.query() == [(2, None, 4), (3, 5, 3), (4, 5, 2), (4, None, 1)]

    links.save(Link(4, 5)) #5
    
    assert links.query(4, 5) == [2, 5]
    assert links.query(7, 7) == []
    
    assert 2 in links
    assert 6 in articles
    
    try:
        articles[99]
        assert False
    except KeyError:
        pass
    try:
        articles['fff']
        assert False
    except KeyError:
        pass
    try:
        links[1010]
        assert False
    except KeyError:
        pass
    
    assert list(l.id for l in links) == [1, 2, 3, 4, 5]
    assert list(l.src_id for l in links) == [4, 4, 3, 2, 4]
    
    
    graph[4] = [2, 3, 4, 5]
    assert graph[4] == [2, 3, 4, 5]
    graph[2] = [3, 2, 1, 6]
    assert graph[2] == [3, 2, 1, 6]



if __name__ == '__main__':
    test()
    print 'all tests passed'
