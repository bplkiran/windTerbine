WindTerbine Project implemented using Pyspark (Python version of Spark).
Followed medallion architecture and stored data in 3 different layers
 1. Bronze : Row data store without any changes in data
 2. Silver: Data cleaned and formatted the data
 3. Gold: Aggrigated data stored in Gold layer.

I currently loaded in csv file format in all the layers. In useally we store data in Delta Lake tables.
