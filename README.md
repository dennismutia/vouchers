# Vouchers Data Engineering Project, Wefarm

Wefarm has voucher data stored in json files. There is need to have this data cleaned and stored in a sql database for easy querying by analysts.

## Requirements

* Python3 
* Pip 
* Python packages
    The requiments.txt file has the relevant packages needed for this project. Run ```pip3 install -r requirements.txt``` to install.
* Postgres database

## How to run
There are several steps involved in runnig this project.

1. Run the command below from a terminal to stage the json files to the tb. Run the command from the root folder of the project.
    ```python3 load_to_db.py```

2. Run dbt to clean the data and build the data models.
    ```
    cd vouchers_project/
    dbt run
    ```
3. To run tests on the models, execute the the command:
    ```
    dbt test
    ```

4. Generate docs and access the web interface for dbt:
    ```
    dbt docs generate
    dbt docs run
    ```