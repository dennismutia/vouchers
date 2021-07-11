# Vouchers Data Engineering Project, Wefarm

Wefarm has voucher data stored in json files. There is need to have this data cleaned and stored in a sql database for easy querying by analysts.

## Requirements

* Postgres database  
* Python3 
* Pip 
* Python packages  
    * The following packages are required for the project to run.
        ```
        pandas
        sqlalchemy
        dbt
        numpy
        ```
    * The requirements.txt file contains a list of these packages. Running the command below installs them at once, and their dependencies.
        ```
        pip3 install -r requirements.txt
        ``` 
*It is preferrable if the above are run in a virtual environment*.

## Data model
The output of this project is two tables.   

Master calendar date dimension  

 field name         | type       | description             
--- | --- | ---  
 date               | datetime   | date field           
 day_of_week        | int        | day of the week         
 day_of_month       | int        | day of the month        
 week_of_month      | int        | week of the month       
 week_of_year       | int        | week of the year        
 month              | int        | month number            
 year               | int        | month number          

Vouchers table 

| field name         | type       | description                 |
| -----------------  |------------|-----------------------      |
| user_id            | int        | id of the user              |
| voucher_code       | binary     | unique id of the voucher    |
| product            | varchar    | name of product             |
| country            | varchar    | vendor country              |
| status             | varchar    | voucher redemption status   |
| date               | datetime   | date associated with voucher|


## How to run
There are several steps involved in runnig this project.

1. Start the postgres database service. For linux based systems, you can use the command below:
    ```
    sudo service postgresql start
    ```

2. Run the command below from a terminal to stage the json files to the tb. Run the command from the root folder of the project.
    ```
    python3 load_to_db.py
    ```

3. Run dbt to clean the data and build the data models.
    ```
    cd vouchers_project/
    dbt run
    ```
4. To run tests on the models, execute the the command:
    ```
    dbt test
    ```

5. Generate and render docs to access the web interface for dbt. By default, dbt will run on port 8080
    ```
    dbt docs generate
    dbt docs run
    ```