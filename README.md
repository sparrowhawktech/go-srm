SRM stands for structure relationship mappin
You can think about it as minimalistic approach to what you may know as an ORM, in go. Only we don't generate every bit of SQL, you write some of it.

As it is truly minimalistic, there are certain rules of engagement and restrictions:

* Everything is an entity. There's no nxm relationship support as we see every relation as atributes to entities (1xn).
* Primary keys are unary and BIGINT (int64)
* Foregin keys are non-nullable.


Check the harness package for self-explanatory usage.
