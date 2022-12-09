# orca-recipes-airflow
This repo will serve as a prototype for basic airflow for the DPE team

## start up
To run airflow, you will need Docker and Docker-Compose installed on your machine.

Once you have the prerequisites installed, you simply need to navigate into the `orca-recipes-airflow` directory and run the following commands.

### Build Docker Containers
```
docker-compose build
```

### Start Docker Containers
```
docker-compose up
```
 - you may want to run the containers in detached mode, so that you can still use the terminal while airflow is running. to do so you need to add `-d` to the end of the command above

 ### Restarting Airflow

When developing, if you make changes to the python code only, you need only to run `docker-compose up` again, but if anything that impacts the docker image changes you must rebuild with `docker-compose build`
