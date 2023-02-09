import synapseclient
from airflow.models import Variable


# create authenticated synapse session
def create_synapse_session():
    syn = synapseclient.Synapse()
    syn.login(
        authToken=Variable.get("SYNAPSE_AUTH_TOKEN")
    )  # TODO - this is currently Brad's synapse token
    return syn
