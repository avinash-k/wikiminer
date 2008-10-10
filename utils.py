'''
Copyright Yongqian Li
'''

import re

def unescape_html(s, re_pattern=re.compile(r'&(#\d+|#x[\da-fA-F]+|[\w.:-]+);')):
    #                                         (dec |hex          |named   )
    #is ; required? if not, use            r'&(#\d+|#x[\da-fA-F]+|[\w.:-]+);?'
    import htmlentitydefs
    def decode_entity(match):
        entity = match.group(1)
        if entity.startswith('#x'): #hex representation
            try:
                return unichr(int(entity[2:], 16))
            except ValueError: #Python "narrow build" does NOT fully support unicode, some code points cannot be represented
                return match.group(0)
        elif entity.startswith('#'): #decimal representation
            try:
                return unichr(int(entity[1:]))
            except ValueError: #Python "narrow build" does NOT fully support unicode, some code points cannot be represented
                return match.group(0)
        else: #named representation
            try:
                return unichr(htmlentitydefs.name2codepoint[entity])
            except KeyError:
                return match.group(0)
                
    s = s.replace('&apos;', '\'')
    return re_pattern.sub(decode_entity, s)
