__author__ = 'jingda'

#返回一個字典key-value，query的value是一個list.一個session有多個query，一個query有多個click
def new_session(session_id, day, user_id):
    return { 'SessionID': int(session_id),
              'Day': int(day),
              'USERID': int(user_id),
              'Query': [] }

#返回一個字典key-value，clicks的value是一個list
def new_query(session_id,time_passed, type_of_record, serp_id, query_id, list_of_terms, url_list):
    return { 'TimePassed': int(time_passed),
              'session_id':int(session_id),
    		  'TypeOfRecord' : str(type_of_record),
              'SERPID': int(serp_id),
              'QueryID': int(query_id),
              'ListOfTerms': tuple(int(t) for t in list_of_terms.split(',')),
              # 'ListOfTerms': [int(t) for t in list_of_terms.split(',')],
              'URL_DOMAIN': [tuple(int(ui) for ui in u.split(',')) for u in url_list],
              'Clicks': [] }

def new_click(time_passed, serp_id, url_id):
    return { 'TimePassed': int(time_passed),
              'SERPID': int(serp_id),
              'URLID': int(url_id) }

def parse(file):
    s = None
    for line in file:
        line2 = line.strip().split('\t')
        if len(line2) == 4 and s is not None:
            yield s
        if len(line2) == 4:
            sid, tor, day, uid = line2
            s = new_session(sid, day, uid)
        elif len(line2) == 16:
            sid, tp, tor, serpid, quid, lot = line2[:6]
            lou = line2[6:]
            if int(sid) == s['SessionID']:
                s['Query'].append(new_query(sid,tp, tor, serpid, quid, lot, lou))
            else:
                print(sid,s['SessionID'],'error not find corresponding session','line16')
                break
        elif len(line2) == 5:
            flag = 0
            sid, tp, tor, serpid, urlid = line2
            if int(sid) == s['SessionID']:
                for query in s['Query']:
                    if int(serpid) == query['SERPID']:
                        query['Clicks'].append(new_click(tp, serpid, urlid))
                        flag += 1
                if flag !=1:
                    print('error not find corresponding SERP',line2)
                    break
            else:
                print('error not find corresponding session',line2)
                break

