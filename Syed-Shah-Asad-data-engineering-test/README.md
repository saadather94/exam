# Addo Spark tasks Repository


The detail and hierarchy of each folder and the files is given below:

- **input:** Contains the recipee files, ready to be used for spark processing.
- **TASK.ipynb:** Notebook contains task implementation on my local system spark enviroment.
- **output:** Beef recipe results, implemented inside Task.ipynb notebook.
- **Azure DataBricks:** Contains results/screenshots/Dashboard of the implementations done on Azure Databricks cluster. 

## Implemeted Task's Overview
For the required tasks, three input recipe files of **_.json_** format are given in input folder for processing. So, for this assignment, i implemented all the tasks first in my local system and its implementation can be seen inside the **[TASK.ipynb]** and the tasks final output is stored in the output folder which contains the recipe of the dishes having only the beef in their ingredients or in their dish name.

Since in real time enviroment all the data/files are being dumped/stored on some cloud based storage. So assuming such a case, i had used Azure databricks and run the spark job on databrick's cluster and its implementation and results are present inside the folder **[Azure DataBricks]**  

The implementation follows as:

- First i stored the input recipee files on my Azure blob storage inside **_data_** container and the final output recipie files based on average cook time and difficulty are stored inside **_output_** container of the same storage namely **_sparkexamstorage_**.

![container](https://github.com/saadather94/exam/blob/Addo_Task/Syed-Shah-Asad-data-engineering-test/Azure%20DataBricks/Azure-StorageContainers-ScreenShot.jpg)


- Then on Databricks i created a spark job that will run the spark script **_[SPARK_JOB_SCRIPT.py]_**.
    - _SPARK_JOB_SCRIPT.py_ has all the required functions implemented required for sucessully completion of the desired output. 
    - So first the libraries and spark session is created followed by my Azure blob storage authentication and accesskey.
    - Then two user defined functions **get_time()** is implemeted that will extract the time in minutes using the regular expression and the other function **extract_serving_value()** is implemented that will get the number of per serving of the dishes, in some records the serving is written as "3 to 7 serving" so in such a case i extracted the numeric values and return the average serving which will be for this case is 5 after rounding off. 
    - Then for the TASK 1 implementation i had defined a function called preprocess() that will do all the preprocessing tasks such as replacing the null values, geting time in minutes, getting the serving into a single numeric entity and removing extra punctuations and getting a same lower case string for dish and ingredients column. This preprcessed dataframe is persisted into the memory for further tasks.
    - Then for TASK 2 total cook time, dish's difficulty level is required. For that **TASK2DF()** function is defined that will do the job. Then the resulting dataframe is used to extract the beef recipies. Then average cooking time is calculated based on per difficulty level and the output result are renamed and stored as a _.csv_ file on the Azure output storage container for each file present inside the input container of storage. 
        
- So to run the spark job preodically i had schedulled it to run every day at 00:00 hrs (12AM) and in case of job failure i will recieve an email. Job completion screenshot can be seen in **[Azure DataBricks]** folder as **_SPARK_JOB_RUN-ScreenShot.jpg_**

![sparkJob](https://github.com/saadather94/exam/blob/Addo_Task/Syed-Shah-Asad-data-engineering-test/Azure%20DataBricks/SPARK_JOB_RUN-ScreenShot.jpg)


The Azure storage output container gets updated after the sucessfull job completion task, for simplicity i have just show a single recipee screenshot below:

![outstorage](https://github.com/saadather94/exam/blob/Addo_Task/Syed-Shah-Asad-data-engineering-test/Azure%20DataBricks/Output-Recipe-ScreenShot.jpg)



- I have also shared some other screenshots related to my Azure storage containers and its final outputs as well inside the Databricks folder.

## Dashboard 

Apart from the spark task implementation, i have also done the analytics on the final beefrecipie and created a dashboard on Databricks it can be seen below as a screen shot inside folder **[Azure DataBricks]**. The dashboard represents the analytics off all the three processed recipe files.

![Dashboard](https://github.com/saadather94/exam/blob/Addo_Task/Syed-Shah-Asad-data-engineering-test/Azure%20DataBricks/FinalBeefRecipe-Dashboard-ScreenShot.jpg)




[TASK.ipynb]: https://github.com/saadather94/exam/blob/Addo_Task/Syed-Shah-Asad-data-engineering-test/TASK.ipynb "Task"
[SPARK_JOB_SCRIPT.py]: https://github.com/saadather94/exam/blob/Addo_Task/Syed-Shah-Asad-data-engineering-test/Azure%20DataBricks/SPARK_JOB_SCRIPT.py "SparkJob"
[Azure DataBricks]: https://github.com/saadather94/exam/tree/Addo_Task/Syed-Shah-Asad-data-engineering-test/Azure%20DataBricks "DataBricks"







