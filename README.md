# 1 Billion Row Challenge in Java

This is the familiar 1 Billion Row challenge that you can find [here](https://1brc.dev/)
and github repo [here](https://github.com/gunnarmorling/1brc).
I've tried to stick to mainstream java in this as I'm not very familiar with
obscure corners of java anyway. But even with that I was able to increase 
performance of this thing from ~28s on my machine (in 1st commit) to ~8s for
100 million records (yup didn't go to full 1B)!

The most 'advanced' java concept used in this is an ExecutorService, and a 
BiFunction. So this should be accessible to anyone who knows basic java and 
googles these two terms.

I do hope to improve on this in future though.

Hope you'll find this useful.

## Instruction for use

The `CreateMeasurements.java` and `CreateMeasurementsFast.java` files are copied
from 1brc repo mentioned above. Run those with required changes (mainly file
location and number of records you need to create). And give proper file path 
in `Main.java` for the program to work correctly.