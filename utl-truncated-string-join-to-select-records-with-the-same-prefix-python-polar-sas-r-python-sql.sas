%let pgm=utl-truncated-string-join-to-select-records-with-the-same-prefix-python-polar-sas-r-python-sql;

Truncated string join to select records with the same prefix python polar sas r python sql

%stop_submission;

A million rows is tiny data?
Not a problem for SAS sql?

         1 sas sql
         2 r sql
         3 python sql
         4 python polar language
           https://stackoverflow.com/users/25260101/ticktalk

github
https://tinyurl.com/26xb6z5z
https://github.com/rogerjdeangelis/utl-truncated-string-join-to-select-records-with-the-same-prefix-python-polar-sas-r-python-sql

stackoverflow python
https://tinyurl.com/2an27wxy
https://stackoverflow.com/questions/79079917/filter-polars-dataframe-where-values-starts-with-any-string-in-a-list

/*               _     _
 _ __  _ __ ___ | |__ | | ___ _ __ ___
| `_ \| `__/ _ \| `_ \| |/ _ \ `_ ` _ \
| |_) | | | (_) | |_) | |  __/ | | | | |
| .__/|_|  \___/|_.__/|_|\___|_| |_| |_|
|_|
*/

/**************************************************************************************************************************/
/*                                     |                                           |                                      */
/*                                     |                                           |                                      */
/*           INPUT                     |  PROCESS (MATCH ON PREFIX)                |                                      */
/*                                     |                                           |                                      */
/*                                     |                                           |                                      */
/*  SD1.PREFIX         SD1.STRING      |       PROCESS                             |     OUTPUT                           */
/*                                     |                                           |                                      */
/*   PREFIX            STRING          |  PREFIX    STRING                         | PREFIX    STRING                     */
/*                                     |                                           |                                      */
/*   123               12345           |            123                            |  123      12345                      */
/*   544               12              |   123      12345    has prefix            |  544      54467899                   */
/*   55443345          54467899        |                                           |                                      */
/*                     54335233        |            544                            |                                      */
/*                                     |   544      54467899 has prefix            |                                      */
/*                                     |                                           |                                      */
/*                                     |                                           |                                      */
/*                                     |   SAME CODE IN SAS, R AND PYTHON          |                                      */
/*                                     |   ==============================          |                                      */
/*                                     |                                           |                                      */
/*                                     |   select                                  |                                      */
/*                                     |      l.prefix                             |                                      */
/*                                     |     ,r.string                             |                                      */
/*                                     |   from                                    |                                      */
/*                                     |      prefix as l inner join string as r   |                                      */
/*                                     |   where                                   |                                      */
/*                                     |     substr(l.prefix,1,length(l.prefix))   |                                      */
/*                                     |       =substr(r.string,1,length(l.prefix))|                                      */
/*                                     |                                           |                                      */
/*                                     |                                           |                                      */
/*                                     |   PYTHON POLAR LANGUAGE                   |                                      */
/*                                     |   =====================                   |                                      */
/*                                     |                                           |                                      */
/*                                     |   res=df[df['STRING'].apply(lambda x: \   |                                      */
/*                                     |     any(x.startswith(prefix) \            |                                      */
/*                                     |     for prefix in prefixes))]             |                                      */
/*                                     |                                           |                                      */
/**************************************************************************************************************************/

/*                   _
(_)_ __  _ __  _   _| |_
| | `_ \| `_ \| | | | __|
| | | | | |_) | |_| | |_
|_|_| |_| .__/ \__,_|\__|
        |_|
*/

options validvarname=upcase;
libname sd1 "d:/sd1";
data sd1.string;
input  string$;
cards4;
12345
12
54467899
54335233
0024355
;;;;
run;quit;

data sd1.prefix;
input  prefix$;
cards4;
123
544
55443345
;;;;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/*  SD1.PREFIX total obs=3                       SD1.STRING total obs=5                                                   */
/*                                                                                                                        */
/*  Obs    PREFIX                                Obs    STRING                                                            */
/*                                                                                                                        */
/*   1     123                                    1     12345                                                             */
/*   2     544                                    2     12                                                                */
/*   3     55443345                               3     54467899                                                          */
/*                                                4     54335233                                                          */
/*                                                                                                                        */
/**************************************************************************************************************************/


/*                             _
/ |  ___  __ _ ___   ___  __ _| |
| | / __|/ _` / __| / __|/ _` | |
| | \__ \ (_| \__ \ \__ \ (_| | |
|_| |___/\__,_|___/ |___/\__, |_|
                            |_|
*/

proc sql;
  create
     table want as
  select
     l.prefix
    ,r.string
  from
     sd1.prefix as l, sd1.string as r
  where
     substr(l.prefix,1,length(l.prefix))
       = substr(r.string,1,length(l.prefix))
;quit;


/*___                     _
|___ \   _ __   ___  __ _| |
  __) | | `__| / __|/ _` | |
 / __/  | |    \__ \ (_| | |
|_____| |_|    |___/\__, |_|
                       |_|
*/

%utl_rbeginx;
parmcards4;
library(sqldf)
library(haven)
source("c:/oto/fn_tosas9x.R")
string<-read_sas("d:/sd1/string.sas7bdat")
prefix<-read_sas("d:/sd1/prefix.sas7bdat")
want <- sqldf('
  select
     l.prefix
    ,r.string
  from
     prefix as l, string as r
  where
     substr(l.prefix,1,length(l.prefix))
       = substr(r.string,1,length(l.prefix))
 ')
want
fn_tosas9x(
      inp    = want
     ,outlib ="d:/sd1/"
     ,outdsn ="sqlwant"
     )
;;;;
%utl_rendx;

proc print data=sd1.sqlwant;
run;quit;


/*____               _   _                             _
|___ /   _ __  _   _| |_| |__   ___  _ __    ___  __ _| |
  |_ \  | `_ \| | | | __| `_ \ / _ \| `_ \  / __|/ _` | |
 ___) | | |_) | |_| | |_| | | | (_) | | | | \__ \ (_| | |
|____/  | .__/ \__, |\__|_| |_|\___/|_| |_| |___/\__, |_|
        |_|    |___/                                |_|
*/


proc datasets lib=sd1 nolist nodetails;
 delete pywant;
run;quit;

%utl_pybeginx;
parmcards4;
exec(open('c:/oto/fn_python.py').read())
string,meta = ps.read_sas7bdat('d:/sd1/string.sas7bdat')
string.info()
prefix,meta = ps.read_sas7bdat('d:/sd1/prefix.sas7bdat')
want=pdsql('''
  select
     l.prefix
    ,r.string
  from
     prefix as l inner join string as r
  where
     substr(l.prefix,1,length(l.prefix))
       = substr(r.string,1,length(l.prefix))
   ''');
print(want)
fn_tosas9x(want,outlib='d:/sd1/',outdsn='pywant',timeest=3)
;;;;
%utl_pyendx;

proc print data=sd1.pywant;
run;quit;


/**************************************************************************************************************************/
/*                                                                                                                        */
/* PYTHON                    SAS                                                                                          */
/*                                                                                                                        */
/*    PREFIX    STRING       PREFIX    STRING                                                                             */
/*                                                                                                                        */
/*  0    123     12345        123      12345                                                                              */
/*  1    544  54467899        544      54467899                                                                           */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*  _                 _   _                               _             _
| || |    _ __  _   _| |_| |__   ___  _ __    _ __   ___ | | __ _ _ __ | | __ _ _ __   __ _ _   _  __ _  __ _  ___
| || |_  | `_ \| | | | __| `_ \ / _ \| `_ \  | `_ \ / _ \| |/ _` | `__|| |/ _` | `_ \ / _` | | | |/ _` |/ _` |/ _ \
|__   _| | |_) | |_| | |_| | | | (_) | | | | | |_) | (_) | | (_| | |   | | (_| | | | | (_| | |_| | (_| | (_| |  __/
   |_|   | .__/ \__, |\__|_| |_|\___/|_| |_| | .__/ \___/|_|\__,_|_|   |_|\__,_|_| |_|\__, |\__,_|\__,_|\__, |\___|
         |_|    |___/                        |_|                                      |___/             |___/


%utl_pybeginx;
parmcards4;
exec(open('c:/oto/fn_python.py').read())
import polar as pl
df,meta = ps.read_sas7bdat('d:/sd1/string.sas7bdat')
prefixes = ['123', '544', '55443345']
print(df)
print(prefixes)
res=df[df['STRING'].apply(lambda x: \
  any(x.startswith(prefix) for prefix in prefixes))]
print(res)
fn_tosas9x(res,outlib='d:/sd1/',outdsn='pywant',timeest=3)
;;;;
%utl_pyendx;


/**************************************************************************************************************************/
/*                                                                                                                        */
/*  PYTHON            SAS                                                                                                 */
/*                                                                                                                        */
/*        STRING      Obs    STRING                                                                                       */
/*                                                                                                                        */
/*   0     12345       1     12345                                                                                        */
/*   2  54467899       2     54467899                                                                                     */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*              _
  ___ _ __   __| |
 / _ \ `_ \ / _` |
|  __/ | | | (_| |
 \___|_| |_|\__,_|

*/
