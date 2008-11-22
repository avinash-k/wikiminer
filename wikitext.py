'''
Copyright Yongqian Li
Released under GPLv3
'''

import re

error_log = open('wikitext_err_log.txt', 'w')

def unquote_wiki(u): ##Otherwise (non-ASCII) convert it first to UTF-8 and then percent-encode it.
    def unpercent(match):
        percents_iter = re.finditer('%([0-9a-fA-F]{2})', match.group())
        try:
            return ''.join(chr(int(hx.group(1), 16)) for hx in percents_iter).decode('utf-8')
        except UnicodeDecodeError:
            print >>error_log, repr(match.group()), 'cannot be decoded', u
            return match.group() #do not replace, return original
    return re.sub('(%[0-9a-fA-F]{2})*', unpercent, u)

def parse_links(text, snippet_len=20, re_pattern=re.compile('''\[\[\s*(?::\s*(?P<colon_escaped_title>.*?)|\s*(?P<unescaped_title>[^:]*?))(?:\|(?P<link_label>.*?))?\]\](?P<link_label2>[^\W\d_]*)''', re.IGNORECASE | re.UNICODE | re.DOTALL)): #handles <nowiki>d</nowiki>
    text = re.sub('<!--.*?-->', '', text) #remove comments
    #note: the pattern WILL match REDIRECTs. Since python re does not allow variable width regexp, it cannot be fixed (elegantly) as of now
    for sre_match in re_pattern.finditer(text):
        link_dest_title = sre_match.group('colon_escaped_title') or sre_match.group('unescaped_title') or '' #'[[:\n]]'
        link_dest_title = unquote_wiki(link_dest_title) #Otherwise (non-ASCII) convert it first to UTF-8 and then percent-encode it.
        frag_match = re.match('(.*)#(.*)$', link_dest_title)
        if frag_match:
            link_dest_title, dest_frag = frag_match.group(1), frag_match.group(2)
        else:
            dest_frag = None
            
        link_dest_title = link_dest_title.strip() #get rid of newlines, etc


        link_label = sre_match.group('link_label') or link_dest_title
        link_label2 = sre_match.group('link_label2')
        if link_label2:
            link_label = link_label + link_label2

        if link_dest_title:
            link_dest_title = link_dest_title[0].upper() + link_dest_title[1:]
            #the first letter of the target page is automatically capitalized by the software. So like this points to the page titled "Like this".
            link_dest_title = link_dest_title.replace('_', ' ')
            #"In links spaces and underscores (_) are treated equivalently."

        span = sre_match.span(0)
        left_snippet = text[max(0, span[0] - snippet_len//2):span[0]]
        right_snippet = text[span[1]:span[1] + snippet_len//2]
        snippet = left_snippet + sre_match.group(0) + right_snippet

        yield link_dest_title, dest_frag, link_label, snippet

    #for sre_match in re.finditer('\[\[\s*:?\s*(.*?)(?:\|(.*?))?\]\]([^ "]*?)', text, re.IGNORECASE):


assert list(parse_links('  [[]]  ')) == [('', None, '', '  [[]]  ')]
assert list(parse_links(u'  [[a\xfc\n]]  ')) == [(u'A\xfc', None, u'a\xfc', u'  [[a\xfc\n]]  ')]
assert list(parse_links('  [[a_b\n]]  ')) == [('A b', None, 'a_b', '  [[a_b\n]]  ')]
assert list(parse_links('[[a]]b0')) == [('A', None, 'ab', '[[a]]b0')]
assert list(parse_links('dbcs d [[0#a|a]][[a]]b0[b]] dd a [[', 10)) == [('0', 'a', 'a', 'cs d [[0#a|a]][[a]]'), ('A', None, 'ab', 'a|a]][[a]]b0[b]]')]
assert list(parse_links('[[a b]]b0')) == [('A b', None, 'a bb', '[[a b]]b0')]
assert list(parse_links('[[:a:b]]b0')) == [('A:b', None, 'a:bb', '[[:a:b]]b0')]
assert list(parse_links('[[a:b]]b0')) == [] #not Wikipedia article link, it is a special link (such as images).
assert list(parse_links('[[:a:B]]b0')) == [('A:B', None, 'a:Bb', '[[:a:B]]b0')]
assert list(parse_links(u'[[:c:B]]b0')) == [(u'c'.upper() + ':B', None, u'c:Bb', u'[[:c:B]]b0')]
assert list(parse_links(u'[[:d:B#abc]]b0')) == [(u'd'.upper() + ':B', u'abc', u'd:Bb', u'[[:d:B#abc]]b0')]
assert list(parse_links(u'aaa[[:c:B#abc|cd]]b0 dfsdg')) == [(u'c'.upper() + ':B', u'abc', u'cdb', u'aaa[[:c:B#abc|cd]]b0 dfsdg')]
assert list(parse_links(u'[[Z%C3%BCrich#a]]')) == [(u'Z\xfcrich', 'a', u'Z\xfcrich', '[[Z%C3%BCrich#a]]')]
assert list(parse_links(u'<!--[[Z%C3%BCrich#a]]-->')) == []
assert list(parse_links('<!-- d -->[[a]]b0')) == [('A', None, 'ab', '[[a]]b0')] #comments are completely stripped out



def parse_redirect(text, re_pattern=re.compile('#REDIRECT\s*:?\s*\[\[\s*:?\s*(.*?)(?:\|(.*?))?\]\]', re.IGNORECASE | re.UNICODE | re.DOTALL)):
    text = re.sub('<!--.*?-->', '', text) #remove comments
    sre_match = re_pattern.search(text)
    if sre_match:
        redirect_dest_title = sre_match.group(1)
        redirect_dest_title = unquote_wiki(redirect_dest_title) #Otherwise (non-ASCII) convert it first to UTF-8 and then percent-encode it.
        frag_match = re.match('(.*)#(.*)$', redirect_dest_title)
        if frag_match:
            redirect_dest_title, dest_frag = frag_match.group(1), frag_match.group(2)
        else:
            dest_frag = None

        redirect_dest_title = redirect_dest_title.strip() #get rid of newlines, etc

        if redirect_dest_title:
            redirect_dest_title = redirect_dest_title[0].upper() + redirect_dest_title[1:]
            #the first letter of the target page is automatically capitalized by the software. So like this points to the page titled "Like this".
            redirect_dest_title = redirect_dest_title.replace('_', ' ')
            #"In links spaces and underscores (_) are treated equivalently."
            

        redirect_label = sre_match.group(2) or redirect_dest_title

        return redirect_dest_title, dest_frag, redirect_label


assert parse_redirect('#REDIRECT [[]]') == ('', None, '')
assert parse_redirect('#REDIRECT [[ab\n]]') == ('Ab', None, 'Ab')
assert parse_redirect('#REDIRECT [[a_b\n]]') == ('A b', None, 'A b')
assert parse_redirect('#REDIRECT [[ab|a]]') == ('Ab', None, 'a')
assert parse_redirect('#REDIRECT [[ab#1|a]]') == ('Ab', '1', 'a')
#assert parse_redirect('#REDIRECT [[:a:b|a]]s') == ('A:b', None, 'as')
assert parse_redirect('#REdDIRECT [[ab]]') == None
assert parse_redirect('#redirect [[Z%C3%BCrich]]') == (u'Z\xfcrich', None, u'Z\xfcrich')
assert parse_redirect('<!--#REDIRECT [[ab|a]]-->') == None
assert parse_redirect('<!-- gg-->#REDIRECT [[ab|a]]') == ('Ab', None, 'a')





'''
if 'REDIRECT' in wikitext and not sre_match:
    print repr(article.title)
    print 'ERROR????????????????????????????????????????????????????????'
    print repr(wikitext[max(0, wikitext.index('REDIRECT')-16):wikitext.index('REDIRECT') + 32])
'''


'''
yongqli@fyynd:~$ tail wikiminer/wikiminer.output
u'riter (d. [[1972]])\n*[[1906]] - [[Y\xf3r'
LINK from: u'April 27' to: u'1972'#None label: u')\n*[[1906'
Article(title=u'1972', revision_id=227362283)
u'[1906]] - [[Y\xf3rgos Theotok\xe1s]], Greek no'
LINK from: u'April 27' to: u'Y\xf3rgos Theotok\xe1s'#None label: None
Article(title=u'Y\xf3rgos Theotok\xe1s', revision_id=226960386)
u'elist (d. [[1966]])\n*[[1913]] - [[Phi'
LINK from: u'April 27' to: u'1966'#None label: u')\n*[[1913'
Article(title=u'1966', revision_id=227321273)
u'[1913]] - [[Philip Hauge Ab
'''
'''
u"'s death, [[Cypria|K\xf9pria]] (unknown "
LINK from: u'Achilles' to: u'Cypria'#None label: u'K\xf9pria'
'''#test unicode
