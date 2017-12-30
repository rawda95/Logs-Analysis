import psycopg2

query1 = '''
  select articles.title,count(*) as views from articles,log 
  where articles.slug = substring(log.path  from 10) 
  group by articles.title 
  order by views DESC 
  limit 3;
  '''

query2 = '''
  select authors.name , count(articles.slug)as views from authors join articles 
  on authors.id = articles.author  
  inner join log
  on articles.slug = substring(log.path  from 10) 
  group by authors.name 
  order by views DESC ;
  '''

query3 = '''
  select a.day ,a.month,a.year,((a.error::float/b.noterror::float)*100)  
  from
  (select EXTRACT(day from log.time) as day,
  EXTRACT(month from log.time) as month,
  EXTRACT(year from log.time) as year,
  count(*)as error from log
  where status like'%404%'
  group by day , month,year)as a
  join
  (select EXTRACT(day from log.time) as day,
  EXTRACT(month from log.time) as month,
  count(*)as noterror from log
  group by day , month ) as b
  on a.day = b.day and a.month = b.month 
  where ((a.error::float/b.noterror::float)*100) >1 ;
  '''


def question1():
    db = psycopg2.connect("dbname=news")
    c = db.cursor()
    # query1
    c.execute(query1)
    q1 = c.fetchall()
    print "the most popular three articles of all time:"
    for i in q1:
        print str(i[0]) + "    - " + str(i[1]) + " views"
    db.close()


def question2():
    db = psycopg2.connect("dbname=news")
    c = db.cursor()
    # query2
    c.execute(query2)
    q2 = c.fetchall()
    print "the most popular article authors of all time:"
    for i in q2:
        print str(i[0]) + "    - " + str(i[1]) + " views"
    db.close()


def question3():
    db = psycopg2.connect("dbname=news")
    c = db.cursor()
    # query3
    c.execute(query3)
    q3 = c.fetchall()
    print "days did more than 1% of requests lead to errors:"
    for i in q3:
        print str(int(i[2])), '-', str(int(i[1])),\
            "-", str(int(i[0])), "  -  ", str(float(i[3])), "% errors"
    db.close()


if __name__ == '__main__':
    question1()
    question2()
    question3()
