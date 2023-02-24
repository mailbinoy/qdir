import consul,re,os,logging,sys,json
LOG_FILE=""
CONFIG={}
#set default values
CONFIG_FILE="/etc/qdir/qdir.conf"

def log_warning(logs):
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    logging.basicConfig(
        filename=LOG_FILE,
        level=logging.WARNING,
        format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
        datefmt='%Y-%m-%dT%H:%M:%S'
        )
    logging.warning(logs)

def log_error(logs):
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    logging.basicConfig(
        filename=LOG_FILE,
        level=logging.WARNING,
        format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
        datefmt='%Y-%m-%dT%H:%M:%S'
        )
    logging.error(logs)

def log_critical(logs):
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    logging.basicConfig(
        filename=LOG_FILE,
        level=logging.WARNING,
        format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
        datefmt='%Y-%m-%dT%H:%M:%S'
        )
    logging.critical(logs)
#    sys.exit(logs)

def read_config(configfile):
    data=load_json(configfile)
    if not os.path.exists(data["config"]["qdir_directory"]):
        log_warning("The qdir config directory, %s does not exist. Creating"%data["config"]["qdir_directory"])
        try:
            os.makedirs(data["config"]["qdir_directory"], exist_ok = True)
            log_warning("Directory '%s' created successfully" %data["config"]["qdir_directory"])
        except OSError as error:
            log_critical("Directory '%s' can not be created" %data["config"]["qdir_directory"])
            sys.exit()
    return data



def load_json(configfile):
    #read the config file
    f=open(configfile)
    #load json
    try:
       data = json.load(f)
    except:
        log_critical("There was an error parsing the config file %s"%configfile)
        data=None
        sys.exit()
    return data


def create_file(filename,data,CONFIG):
    filename = filename.replace("qdir/1.0.0/bootstrap/",'')
    path = os.path.join(CONFIG["config"]["qdir_directory"], filename)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    f = open(path, 'wb')
    f.write(data['Value'])
    f.close()

def main():
    CONFIG=read_config(CONFIG_FILE)
    LOG_FILE=CONFIG["config"]["log_file"]
    try:
        consul_client = consul.Consul(host=CONFIG["config"]["qconsul_address"],port=CONFIG["config"]["qconsul_port"],scheme=CONFIG["config"]["qconsul_protocol"],verify=False)
    except:
        log_critical("Cannot connect to consul %s:%s"%(CONFIG["config"]["qconsul_address"],CONFIG["config"]["qconsul_port"]))
    try:
        index, data = consul_client.kv.get('qdir/1.0.0/bootstrap/', keys=True)
    except:
        log_critical("Cannot read %s path in consul %s"%('qdir/1.0.0./bootstrap',CONFIG["config"]["qconsul_address"]))
    try:
        for file in data:
            index, data = consul_client.kv.get(file)
            create_file(file,data,CONFIG)
    except:
        pass
