'''This node is automatically created for convenience and can be safely deleted or disabled once in production.'''
#
# This node clones the official nodel-recipes repository at: github.com/museumsvictoria/github
#
# Once cloned, new nodes can be easily created or updated based on the latest recipes.
#
# Deleting this node does not delete the actual recipes repository.

from org.nodel.jyhost import NodelHost
from org.eclipse.jgit.api import Git
from java.io import File

DEFAULT_NAME = "nodel-official-recipes"
DEFAULT_URI = "https://github.com/museumsvictoria/nodel-recipes"
param_repository = Parameter({'title': 'Repository', 'schema': {'type': 'object', 'properties': {
        'name': {'type': 'string', 'hint': DEFAULT_NAME, 'order': 1},
        'uri': {'type': 'string', 'hint': DEFAULT_URI, 'order': 2}}}})

# sync every 72 hours, first after 10 seconds
sync_every_day = Timer(lambda: lookup_local_action("sync").call(), 72*3600, 10)

# the internet address
uri = DEFAULT_URI

# the nodel-recipes repo folder
folder = File(NodelHost.instance().recipes().getRoot(), DEFAULT_NAME)

def main():
  if param_repository != None:
    global uri, folder
    
    uri = param_repository.get('uri') or DEFAULT_URI
    name = param_repository.get('name') or DEFAULT_NAME
    folder = File(NodelHost.instance().recipes().getRoot(), name)

def local_action_Sync(arg=None):
  sync()

def sync():
  clone_if_necessary()
  pull()
  
def clone_if_necessary():
  if folder.exists():
    return
  
  console.info("Cloning %s..." % uri)
  
  try:
    git = None
    
    cmd = Git.cloneRepository()
    cmd.setURI(uri)
    cmd.setDirectory(folder)
    git = cmd.call()
    
    console.info("Cloning finished")
    
  finally:
    if git != None: git.close()
    
def pull():
  if not folder.exists():
    console.warn('repo folder does not exist; yet to clone or network issues?')
  
  console.info("Pulling repository...")
  
  try:
    git = None
    git = Git.open(folder)
    git.pull().call()
    
    console.info("Pull finished")
    
  finally:
    if git != None: git.close()
