SRM stands for struc relationship mapping.
You can think about it as minimalistic approach to what you may know as an ORM, in go. Only we don't generate SQL sentences, you write them.

As it is truly minimalistic, there are certain rules of engagement and restrictions:

* Everything is an entity. There's no nxm relationship support as we see every relation as atributes to entities (1xn).
* Primary keys are unary and BIGINT (int64)


Check the harness package for self-explanatory usage.
