'''
Copyright Yongqian Li
'''


class Redirect(object):
    def __init__(self, dest_id=None, dest_fragment=None, label=None):
        self.dest_id = dest_id
        self.dest_fragment = dest_fragment
        self.label = label
    
    def __repr__(self):
        return "Redirect(title='%s', article=A('%s'))" % (self.title, self.article.title)


class Article(object):
    def __init__(self, id, title=None, revision_id=None, redirect=None, vertex_id=None):
        self.id = id
        self.title = title
        self.revision_id = revision_id
        self.redirect = redirect
        self.vertex_id = vertex_id


    def __repr__(self):
        return "Article(id=%s, title=%s, revision_id=%s)" % (repr(self.id), repr(self.title), self.revision_id)
        

class Link(object):
    def __init__(self, id, src_id, dest_id=None, dest_fragment=None, label=None, snippet=None):
        self.id = id
        self.src_id = src_id
        self.dest_id = dest_id
        self.dest_fragment = dest_fragment
        self.label = label
        self.snippet = snippet










import os.path
current_dir = os.path.dirname(os.path.abspath(__file__))










import bsddb
import pickle

'''
title (utf-8) -> article_id
article_id -> Article object
redirect_id -> Redirect object
link_id -> link_object
link_index: article_id, article_id -> [link_id]

vertex_id -> article_id
#adj_list: vertex_id -> [vertex_id]
'''


title_to_aid_db = bsddb.hashopen('title_to_aid.db', flag='c')
aid_to_article_db = bsddb.rnopen('aid_to_article.db', flag='c')
#rid_to_redirect_db = bsddb.rnopen('rid_to_redirect.db', flag='c')
lid_to_link_db = bsddb.rnopen('lid_to_link.db', flag='c')

link_index_db = bsddb.btopen('link_index.db', flag='c')
vid_to_aid_db = bsddb.rnopen('vid_to_aid.db', flag='c')
adjacency_list_db = bsddb.rnopen('adjacency_list.db', flag='c')

def title_to_aid(title):
    key = title.encode('utf-8')
    if key in title_to_aid_db:
        return int(title_to_aid_db[key])
    else:
        return None

def load_article(aid):
    key = aid
    return pickle.loads(aid_to_article_db[key])
    
'''
def load_redirect(rid):
    key = rid
    return pickle.loads(rid_to_redirect_db[key])
'''

def load_link(lid):
    key = lid
    return pickle.loads(lid_to_link_db[key])
    
def find_lids(aid_1, aid_2):
    key = '%s %s' % (aid_1, aid_2)
    lids = []
    k, v = link_index_db.set_location(key)
    while True:
        if k.startswith(key):
            lids.append(int(k[len(key)+1:]))
            k, v = link_index_db.next()
        else:
            break
    return lids

def vid_to_aid(vid):
    key = vid
    return int(vid_to_aid[key])

def get_adjacent_vids(vid):
    key = vid
    v = adjacency_list_db[key]
    return [int(x) for x in v.split()]



def save_article(article):
    key = article.title.encode('utf-8')
    title_to_aid_db[key] = str(article.id)
    key = article.id
    aid_to_article_db[key] = pickle.dumps(article, pickle.HIGHEST_PROTOCOL)
    
'''
def save_redirect(redirect):
    key = redirect.id
    rid_to_redirect_db[key] = pickle.dumps(redirect)
'''

def save_link(link):
    key = link.id
    lid_to_link_db[key] = pickle.dumps(link, pickle.HIGHEST_PROTOCOL)
    key = '%s %s %s' % (link.src_id, link.dest_id, link.id)
    link_index_db[key] = None

def save_vertex(vid, aid, adjacent_vids):
    vid_to_aid_db[vid] = aid
    adjacency_list_db[vid] = ' '.join(str(v) for v in adjacent_vids)
    
def sync():
    title_to_aid_db.sync()
    aid_to_article_db.sync()
    #rid_to_redirect_db.sync()
    lid_to_link_db.sync()
    link_index_db.sync()
    vid_to_aid_db.sync()
    adjacency_list_db.sync()
    
def clear():
    title_to_aid_db.clear()
    aid_to_article_db.clear()
    #rid_to_redirect_db.clear()
    lid_to_link_db.clear()
    link_index_db.clear()
    vid_to_aid_db.clear()
    adjacency_list_db.clear()
    