import mysql.connector
import sys
import webbrowser
from scrap import fetch_news
from hanziconv import HanziConv
if len(sys.argv) == 1:
    command = 'help'
else:
    command = sys.argv[1]
args = sys.argv[2:]

news_db = mysql.connector.connect(
    host='localhost',
    user='root',
    database='news'
)
cursor = news_db.cursor()
extra = ''
cursor.execute("set session sql_mode=''")
text = 'text'
if command == 'search' or command == 's':
    if len(args) == 0:
        print('expect a keyword to search')
        exit(1)
    if 'within' in args: # within days
        try:
            within = int(args[args.index('within')+1])
        except:
            print("Require integer after 'within'")
            exit(1)
        extra += f' AND (DATEDIFF(NOW(), date)) <= {within}'
    if 'from' in args: # search by source
        try:
            source = args[args.index('from')+1]
            if 'exactly' in args:
                cond = f'="{source}"'
            else:
                cond = f'LIKE "%{source}%"'
        except:
            print("Require news source after 'from'")
            exit(1)
        extra += f' AND source {cond}'
    if 'sort' in args:
        try:
            sort_col = args[args.index('sort')+1]
            if 'desc' in args:
                sort_order = 'desc'
            else:
                sort_order = 'asc'
        except:
            print("Require column name and order after 'sort'")
            exit(1)
        extra += f' ORDER BY {sort_col} {sort_order}'
    if 'chs' in args:
        args[0] = HanziConv.toSimplified(args[0]) # convert tp CHS
    if 'cs' in args:
        title = 'title'
        pattern = args[0]
    else:
        title = 'LOWER(title)'
        pattern = args[0].lower()
    if 'exactly' not in args:
        pattern = f'%{pattern}%'
    if 'summary' in args:
        text = 'summary'
    command = f'SELECT source, date, title, url, {text} FROM news WHERE {title} LIKE "{pattern}"' + extra
    print(f'Execute "{command}"')
    cursor.execute(command)
    results = cursor.fetchall()
    if len(results) == 0:
        print('Nothing found')
        exit(0)
    else:
        i = 0
        for _ in results:
            if _[1] is None:
                print(f'{i}. {_[0]}: {_[2]}')
            else:
                print(f'{i}. {_[0]}: {_[2]} ({_[1]})')
            i += 1
        if len(results) > 1:
            print(f'{i}. Cancel')
            choice = int(input('Which: '))
            while (choice < 0 or choice > len(results)):
                print('out of range')
                choice = int(input('Which: '))
        else:
            choice = 0
        if choice == len(results): # cancel
            exit(0)
        result = results[choice]
        print(f'URL: {result[3]}')
        if len(result[4]) == 0:
            ask = input('No content is stored. Open it in web browser? (y/n): ')
            if ask.lower() == 'y':
                webbrowser.open(result[3])
        else:
            content = result[4]
            if type(content) == bytes:
                print(content.decode('UTF-8'))
            else:
                print(content)

elif command == 'count' or command == 'c':
    col_name = '*'
    if 'by' in args:
        try:
            col_name = args[args.index('by')+1]
        except:
            print('expect column name after count by')
            exit(1)
        command = f'SELECT {col_name}, COUNT(*) FROM news'
        extra += f' GROUP BY {col_name}'
    elif 'source' in args:
        col_name = 'source'
        try:
            source = args[args.index('source')+1]
        except:
            print('expect column name after count source')
            exit(1)
        command = f'SELECT source, COUNT(*) FROM news where source LIKE "%{source}%"'
    else:
        command = 'SELECT COUNT(*) FROM news'
    if 'within' in args: # within days
        try:
            within = int(args[args.index('within')+1])
        except:
            print("Require integer after 'within'")
            exit(1)
        extra += f' WHERE (DATEDIFF(NOW(), date)) <= {within}'
    if 'sort' in args:
        if 'desc' in args:
            sort_order = 'desc'
        else:
            sort_order = 'asc'
        extra += f' ORDER BY {col_name} {sort_order}'
    command += extra
    print(f'Execute "{command}"')
    cursor.execute(command)
    results = cursor.fetchall()
    for _ in results:
        if len(_) > 1:
            count = _[1]
            col = _[0]
        else:
            count = _[0]
            col = '*'
        print(f'{count:6} articles from {col_name} {col}')
elif command == 'list' or command == 'l':
    if len(args) == 0:
        print("Require news source after 'list'")
        exit(1)
    command = f'SELECT source, title FROM news WHERE source LIKE "%{args[0]}%"'
    print(f'Execute "{command}"')
    cursor.execute(command)
    results = cursor.fetchall()
    for result in results:
        print(result[1])
elif command == 'fetch' or command == 'f':
    fetch_news()
elif command == 'help' or command == 'h':
    print('''
news-query.py command [arguments] [options]
commands:
    - c[ount]                    count news
    - s[earch]                   search for news
    - l[ist]                    list all news of a specific source
    - f[etch]                   fetch news
    - h[elp]                    print this help message
count:
    [options]:
        - within N_DAYS         count news within N days (override 'by' option)
        - by COLUMN             count news by column
        - source SOURCE         count news of specific source
        - sort [ORDER]          sorting order [asc | desc] (default: asc)
search:
    arguments:
        - KEYWORD               keyword to search
    [options]:
        - within N_DAYS         search news within N days
        - from SOURCE           search news from specific source
        - sort COLUMN [ORDER]   sorting order [asc | desc] (default: asc)
        - chs                   search in Simplified Chinese
        - cs                    case-sensitive search
        - exactly               search with no wildcard
        - summary               display summary instead of content
list:
    argument:
        - SOURCE_NAME       name of the news source
fetch: No arguments
    ''')
else:
    print(f'Unknown command "{command}"')
    exit(1)