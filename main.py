#!/bin/env python3
import logging,os,sys,json,subprocess,time
from pathlib import Path
from datetime import datetime
from argparse import ArgumentParser
import setup_consul_backend
CONFIG={}
#set default values
CONFIG_FILE="/etc/qdir/qdir.conf"
LOG_FILE=""


def log_warning(logs):
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    logging.basicConfig(
        filename=LOG_FILE,
        level=logging.WARNING,
        format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
        datefmt='%Y-%m-%dT%H:%M:%S'
        )
#    logging.Formatter("%(asctime)s;%(levelname)s;%(message)s")
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
#    logging.Formatter("%(asctime)s;%(levelname)s;%(message)s")
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
#    logging.Formatter("%(asctime)s;%(levelname)s;%(message)s")
    logging.critical(logs)


def read_params():
    parser = ArgumentParser()
    parser.add_argument("-c", "--configfile", type=str,help="Force a run for a config", default=None, required=False)
    args = parser.parse_args()
    FORCE_RUN_CONFIG=args.configfile
    return {"force_run_config": FORCE_RUN_CONFIG}

def read_config(configfile):
    data=load_json(configfile)
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
    return data





def run_ansible(module,file,config,qdir_directory):
    global_vars=CONFIG["config"]["qdir_directory"]+"/globalvars.yaml"
    f = open(ANSIBLE_LOG_FILE, "a")
    if module == "templates":
        args ='ansible-playbook qdir_ansible_template.yml -i localhost, -e ansible_python_interpreter=/bin/python --extra-vars @'+str(global_vars)+' --extra-vars @' + str(qdir_directory)+'/vars.yaml -e src=' + str(qdir_directory)+'/'+str(file) + ' -e dest=' + config["path"] + ' -e owner=' + config["owner"] + ' -e group=' + config["group"] + ' -e mode=' + config["mode"] + ' -e exec=\'"'+config["reload"]+'"\''
        f.write("%s QDIR : Template %s\n"%(datetime.now().isoformat(timespec='milliseconds'),str(qdir_directory)+'/'+str(file)))
        f.write("Executing | %s\n"%args)
        f.flush()

        proc=subprocess.call(args, shell=True,stdout=f)
    elif module == "files":
        args ='ansible-playbook qdir_ansible_file.yml -i localhost, -e ansible_python_interpreter=/bin/python --extra-vars @'+str(global_vars)+' --extra-vars @' + str(qdir_directory)+'/vars.yaml -e src=' + str(qdir_directory)+'/'+str(file) + ' -e dest=' + config["path"] + ' -e owner=' + config["owner"] + ' -e group=' + config["group"] + ' -e mode=' + config["mode"] + ' -e exec=\'"'+config["reload"]+'"\''
        f.write("%s QDIR : File %s\n"%(datetime.now().isoformat(timespec='milliseconds'),str(qdir_directory)+'/'+str(file)))
        f.write("Executing | %s\n"%args)
        f.flush()

        proc=subprocess.call(args, shell=True,stdout=f)
    f.close()




def qdir(configfile,qdir_directory=""):
    if (qdir_directory == ""):
        qdir_directory = Path(configfile).parent
    data=load_json(configfile)
    try:
        data["templates"]
        if data["templates"]:
            for i in data['templates']:
                config = check_config(i,data['templates'][i])
                if config == None:
                    log_warning("Skipping config %s:%s"%(configfile,i))
                else:
                    run_ansible("templates",i,config,qdir_directory)
    except:
        pass
    try:
        data["files"]
        for i in data['files']:
            config = check_config(i,data['files'][i])
            if config == None:
                log_warning("Skipping config %s:%s"%(configfile,i))
            else:
                run_ansible("files",i,config,qdir_directory)
    except:
        pass
    return True

def check_config(i,config):
    try:
        config['mode']
    except:
        log_warning("mode missing for config %s. Defaulting to 644"%i)
        config['mode']=644
    try:
        config['path']
    except:
        log_error("path is missing.Cannot process %s directive"%i)
        return None
    try:
        config['reload']
    except:
        log_warning("reload missing for config %s. Ignoring"%i)
        config['reload']=""
    try:
        config['owner']
    except:
        log_warning("owner missing for config %s. Defaulting to root"%i)
        config['owner']="root"
    try:
        config['group']
    except:
        log_warning("group missing for config %s. Defaulting to root"%i)
        config['group']="root"
    return config







if __name__ == '__main__':
#    global CONFIG
    CONFIG=read_config(CONFIG_FILE)
#   log files
    ANSIBLE_LOG_FILE=CONFIG["config"]["ansible_log_file"]
    LOG_FILE=CONFIG["config"]["log_file"]
#   read command line parameters
    PARAMS=read_params()
    if PARAMS['force_run_config'] is not None:
        log_warning("Force run : %s"%PARAMS['force_run_config'])
        data=qdir(PARAMS['force_run_config'])
        if data == None:
            sys.exit("Qdir failed to load config %s"%PARAMS['force_run_config'])
        sys.exit("Complete")
#   check backend
    if CONFIG["config"]["backend"] == "qconsul" or CONFIG["config"]["backend"] == "consul":
        setup_consul_backend.main()
    try:
        qdir_directory = Path(CONFIG["config"]["qdir_directory"])
    except:
        log_critical("Unable to find qdir_directory")
        sys.exit("Unable to find qdir_directory")

    while True:
        if CONFIG["config"]["backend"] == "qconsul" or CONFIG["config"]["backend"] == "consul":
            setup_consul_backend.main()
        try:
            qdir_directory = Path(CONFIG["config"]["qdir_directory"])
        except:
            log_critical("Unable to find qdir_directory")
            sys.exit("Unable to find qdir_directory")        
        for qdir_module in qdir_directory.iterdir():
            if qdir_module.is_dir():
                files = qdir_module.glob('*.conf')
                for file in files:
                    with file.open('r') as file_handle :
                        data=qdir(file_handle.name,str(qdir_module))
                        if data == None:
                            log_critical("Qdir failed to load")
        time.sleep(CONFIG["config"]["delay"])
