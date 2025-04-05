## Task 1. ETL pipeline code

  Build docker-compose.yml:

    What it does:
      1. should install all the pre-requisite environment needed to run/complete the assignment.
      2. should setup the environment to execute this assignment

  Run the etl job using the command:
```
    docker-compose run etl python main.py   --source /opt/data/transaction.csv   --destination /opt/data/out.parquet
```
  Above should run the query on the data provided and should generate desired results in parquet format.

  Your job in this assignment:

    - Task-1:
      How would you read or analyse the results of above query?
      Also share the results produced in readable format(csv)
      Bonus:
        How would you improvise the documentation of the code provided, and document it better for readability?

    - Task-2:
      Make changes to save the results additionally in postgres in a database called "warehouse" and a table called "customers"
      The following command should be executable:
          docker-compose run etl python main.py   --source /opt/data/transaction.csv   --database warehouse   --table customers
          Hint:
            - Make the required changes in:
              - etl_jobs/EtlJobForSertis.py (You may use the 'pass' Python keyword left in the code as a helping marker)
              - docker-compose (to add postgres service)
              - Dockerfile (optional to add missing dependencies)
              - You may rename .env.sample to .env and use the env variables in it

### Submission
Implement the requirements in this Git repository and create a [patch file](https://git-scm.com/docs/git-format-patch) for the changes. Include the patch file with your submitted documents.

  ## Task 2. System Architecture

  ### Preparation

  This task does not have to be implemented. Imagine you write a proposal to us for data warehouse solution on Cloud.
  Choose one of the major cloud platforms, *Amazon Web Services*, *Google Cloud Platform* or *Microsoft Azure* as a basis of your solution and use services provided by the chosen platform.

  ### Requirements
  Describe what technolgoies would you choose and why, in order to build your proposed architecture

  #### Bonus:

  * Propose solutions on an additional cloud platform, covering the same requirements

  ### Submission
  Explanation of the proposed solution, chosen tools/frameworks, justification on why you chose them, and any other supporting documentation, in PDF or markdown format.
