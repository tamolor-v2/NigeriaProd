import os,fnmatch,argparse,sys,json,re,time,datetime,csv
from utils import LoggerManager
from utils import SSHConnection
import subprocess


logger = LoggerManager.LoggerManager("Main")

#reading the config from the Json file
def readConfFile(jsonFile):
    global names
    global commands
    global versions
    global flarePaths
    global successfullInstalation
    global unsuccessfullInstalation
    global ranges
    global fdi
    successfullInstalation = []
    unsuccessfullInstalation = []
    with open(jsonFile) as f:
        info = json.load(f)
    nodes = info['info']['nodes']
    fdi = info['info']['FDI']
    names = info['info']['Names']
    commands = info['info']['Commands']
    versions = info['info']['Versions']
    flarePaths = info['info']['FlarePaths']
    ranges = info['info']['Ranges']
    nodesQueue = []
    #filling the nodes in queue
    for nd in nodes:
        nodesQueue.append(nd)
    return nodesQueue

def getLeader(nodesQueue, con):
    global leader
    leader = None
    leaderId = None
    for node in nodesQueue:
        if node["status"] == "active":
            cmd = "cat {0}/Engine_Node{1}.log |grep -i -o -E leader:NodeId\-[0-9]+\-|tail -1|grep '\-[0-9]+\-' -E -o|sed 's/\-//g'".format(flarePaths["LogPath"], node["NodeId"])
            # stdout = con.runCommand(ssh_client,cmd)
            stdout = runCmdLocal(cmd)
            #check if the command returns the node id, and break the loop
            if stdout:
                leaderId = stdout.replace('\n','')
                break
           #con.closeConnection(ssh_client, node["NodeIpAddr"])
    #checks if we got the leader form the nodes or not, if yes it will remove the node from the queue and return its value
    logger.info(nodesQueue)
    if leaderId:
        for node in nodesQueue:
            if node["NodeId"] == leaderId:
                leader = node
                nodesQueue.remove(node)
                break
        if leader is None:
            logger.error("leader node: %s is not found in the config file!"%leaderId)
            sys.exit(1)
        return leader,nodesQueue
    else:
        logger.error("leader node not found in the logs!")
        sys.exit(1)


def upgrade_cluster(con, nodesQueue):
    logger.info("Removing old version links from the Cluster.....")
    for node in nodesQueue:
        unlink(con, node['NodeIpAddr'])

    logger.info("Preparing and linking the jars.....")
############
    getClusterConfig(nodesQueue)
    runCmdLocal("sed -i -E 's/\-[0-9]+\.[0-9]+\.[0-9]\.jar/\.jar/g' {0}/{1}".format(flarePaths['ClusterConfig'], ClusterConfig))
    runCmdLocal("cp {1} {0}/ClusterInstall/MetadataAPIConfig.properties".format(flarePaths['Working_Dir'], flarePaths['MetadataAPIConfig_Path']))
    sedMeta = "sed -i -E 's/NODEID.*/{1}/g; s/\-[0-9]+\.[0-9]+\.[0-9]//g' {0}/ClusterInstall/MetadataAPIConfig.properties".format(flarePaths["Working_Dir"], "NODEID={NodeId}")
    runCmdLocal(sedMeta)
###########
    logger.info("Upgrading The Cluster.....")
    upgradeCmd = commands["upgradeNode"] %{ "Working_Dir": flarePaths["Working_Dir"] + "/ClusterInstall","kamanja_new": versions['kamanja_new'], "TarBall": flarePaths["TarBall"], "Tenant_Id": names["Tenant_Id"], "Cluster_Id": names["Cluster_Id"], "Link": soft_link, "ClusterCnfPath": flarePaths['Working_Dir'] + "/ClusterInstall", "Environment": names["Environment"] }
    runCmdLocal(upgradeCmd)

    logger.info("Preparing and linking the jars.....")
    prepareCluster(con, nodesQueue, True)

    logger.info("Stopping the Cluster")
    cluster_controller("Cluster", "stop")

    logger.info("Stopping the Cluster")
    cluster_controller("Cluster", "force_stop")

    logger.info("Starting the Cluster")
    cluster_controller("Cluster", "start")


#starting the upgrade process on the node
def upgradeNode(node, con):
    ssh_client = con.connect(node["NodeIpAddr"])
    logger.info("=======================================================")
    cmd = "realpath %s" %flarePaths["Install_Path"]
    stdout = con.runCommand(ssh_client, cmd)
    logger.info(stdout)
    if "{0}.{1}".format(versions['kamanja_new'], versions['build_version']) in stdout[0]:
        logger.info("node %s is already upgraded!"%node['NodeIpAddr'])
        return


    if node["status"] == "active":
        has_grid = stopNode(node, con, "GRID")
        has_kamanja = stopNode(node, con, "KAMANJA")
        has_validator = stopNode(node, con, "VALIDATOR")

    has_fdi = False
    if fdi['NodeId'] == node['NodeId']:
        stopNode(node, con, "FDI")
        has_fdi = True


    #replace vars
    setPrams(node)
    #unlink old path
    real_path = unlink(con, node["NodeIpAddr"])
    #upgrade Node
    upgradeCmd = commands["upgradeNode"] %{ "Working_Dir": flarePaths["Working_Dir"] + "/ClusterInstall","kamanja_new": versions['kamanja_new'], "TarBall": flarePaths["TarBall"], "Tenant_Id": names["Tenant_Id"], "Cluster_Id": names["Cluster_Id"], "Link": soft_link, "ClusterCnfPath": flarePaths['Working_Dir'] + "/ClusterInstall", "Environment": names["Environment"]}
    logger.info("start the upgrade::")
    runCmdLocal(upgradeCmd)
    logger.info("upgrade has been finished")
    logger.info("linking the jars...")

    # ssh_client = con.connect(node["NodeIpAddr"])
    cmd = linkJars("%s/lib/system"%flarePaths['Install_Path'])
    con.runCommand(ssh_client, cmd)
    ipAddr = con.runCommand(ssh_client, "hostname -i")
    con.closeConnection(ssh_client, node["NodeIpAddr"])

    #check if the node in active mode, if yes it will start kamanja on it
    if node["status"] == "active" and not has_fdi:
        controller(node['NodeIpAddr'], "start", ipAddr[0])

    if has_fdi:
        fdi_controller(node)

    logger.info("##-##-##-##-##-##-##-##-##-##-##-##-##-##-##-##-##-##-##-##-##-##-##-##-##-##-##-##-##-##-##-##-##-##-##-##-##-##-##-")
    validate_installation(node, con, real_path[0].replace("\n",""), has_grid, has_kamanja, has_validator, has_fdi)

#unlink installation Directory
def unlink(con, node):
    ssh_client = con.connect(node)
    cmd = "realpath %s" %flarePaths["Install_Path"]
    realpath = con.runCommand(ssh_client, cmd)
    con.runCommand(ssh_client,"rm %s" %flarePaths["Install_Path"])
    con.closeConnection(ssh_client, node)
    return realpath


def rollback(con, audit_path):
    with open(audit_path) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter='|')
        line_count = 0
        for row in csv_reader:
            logger.info("NodeIpAddr: {0} \tisleader: {1} \tinstallation_status: {2} \nfdi_status: {3} \tgrid_status: {4} \nkamanja_status: {5} \nvalidator_status: {6} \treal_path: {7} \tnew_path: {8}".format(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8]))
            line_count += 1
            if row[2]  and versions['kamanja_old'] in row[8]:
                logger.info("downgrade Flare on: %s"%row[0])
                ssh_client = con.connect(row[0])
                unlink(con, row[0]) ##unlink the new installed version
                cmd = "ln -sf {0} {1}".format(row[7], flarePaths['Install_Path'])
                con.runCommand(ssh_client, cmd)
                con.closeConnection(ssh_client, row[0])


def rollbackCluster(con, nodes, old_instance):
    for node in nodes:
        ssh_client = con.connect(node["NodeIpAddr"])
        cmd = "realpath %s" %flarePaths["Install_Path"]
        realpath = con.runCommand(ssh_client, cmd)
        if versions["kamanja_new"] in realpath:
            logger.info("Unlink this version: %s " %realpath)
        else:
            logger.info("This node doesn't have the new version")
        con.runCommand(ssh_client, "rm %s" %flarePaths["Install_Path"])

        logger.info("Linking the old version %s" %old_instance)
        cmd = "ln -sf {0} {1}".format(old_instance, flarePaths['Install_Path'])
        con.runCommand(ssh_client, cmd)
        con.closeConnection(ssh_client, node["NodeIpAddr"])


#runs the command in the local host (the host that you are running the script from)
def runCmdLocal(cmd):
    logger.info("running %s ."%cmd)
    process = subprocess.Popen(cmd, shell=True,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    if stderr:
      logger.error("error caused by: "+str(stderr))
    logger.info("command succeded: "+str(stdout))
    return stdout

def linkJars( path):
    cmd = """
install_dir={0}
echo "Creating links for jars in folder $install_dir"
files_array=(`find $install_dir -type f -name '*.jar' | egrep -e '\-[0-9]+\.[0-9]+\.[0-9]+\.[0-9]{{4}}\.jar$' -e '\-[0-9]+\.[0-9]+\.[0-9]+\.jar$'`)
for src_fl in ${{files_array[@]}}
do
    lnk_fl=$(sed -e 's#\(.*\)\(\-[0-9]\+\.[0-9]\+\.[0-9]\+\.[0-9]\{{4\}}\.jar$\)#\\1.jar#g' -e 's#\(.*\)\(\-[0-9]\+\.[0-9]\+\.[0-9]\+\.jar$\)#\\1.jar#g' <<< $src_fl)
    echo  $src_fl $lnk_fl
    ln -sf $src_fl $lnk_fl
done
    """.format(path)
    return cmd


def validate_installation(node, con, real_path, has_grid, has_kamanja, has_validator, has_fdi):
    ssh_client = con.connect(node["NodeIpAddr"])
    grid_status = "-"
    kamanja_status = "-"
    validator_status = "-"
    fdi_status = "-"
    isleader = False

    if leader['NodeId'] == node['NodeId']:
        isleader = True

    if has_grid:
        grid_status = validate_service(ssh_client, con, "GRID")
    if has_kamanja:
        kamanja_status = validate_service(ssh_client, con, "KAMANJA")
    if has_validator:
        validator_status = validate_service(ssh_client, con, "VALIDATOR")
    if has_fdi:
        fdi_status = validate_service(ssh_client, con, "FDI")


    new_path = "{0}_{1}.{2}_{3}".format(flarePaths['Install_Path'], versions['kamanja_new'], versions['build_version'], date)
    installation_cmd = "ls -d /{0}".format(new_path)
    installation_status = con.runCommand(ssh_client, installation_cmd)
    logger.info(installation_status)
    audit_line = []

    if installation_status:
        installation_status = True
        successfullInstalation.append(node['NodeIpAddr'])
    else:
        installation_status = False
        unsuccessfullInstalation.append(node['NodeIpAddr'])
    isleader = False

    if leader['NodeId'] == node['NodeId']:
        isleader = True

    audit_line = [node['NodeIpAddr'],isleader,installation_status,fdi_status, grid_status, kamanja_status, validator_status, real_path, new_path]

    audit(audit_line)
    con.closeConnection(ssh_client, node["NodeIpAddr"])

    flag = False

    if has_grid and not grid_status:
        logger.error("grid sevice has not started yet %s"%node["NodeIpAddr"])
        flag = True
    if has_kamanja and not kamanja_status:
        logger.error("kamanja sevice has not started yet %s"%node["NodeIpAddr"])
        flag = True
    if has_validator and not validator_status:
        logger.error("validator sevice has not started yet on %s"%node["NodeIpAddr"])
        flag = True
    if has_fdi and not fdi_status:
        logger.error("fdi sevice has not started yet on %s"%node["NodeIpAddr"])
        flag = True
    if not installation_status:
        logger.error("the new version has not installed successfully on node %s"%node["NodeIpAddr"])
        flag = True

    if flag:
        logger.error("not all the services are running fine!, will exit..")
        sys.exit(1)


def validate_service(ssh_client, con, service):
    hostIp = con.runCommand(ssh_client, "hostname -i")
    hostIp = hostIp[0]
    nRange = hostIp[len(hostIp)-4:len(hostIp)+1]
    sv = ""
    if service == "KAMANJA":
        sv = "kamanjamanager_2.11"
        logger.info("Check KAMANJA")
    elif service == "GRID" and int(nRange) in gridR:
        sv = "com.ligadata.Grid.GridServerInstance"
        logger.info("Check GRID")
    elif service == "VALIDATOR" and int(nRange) in validatorR:
        sv = "com.ligadata.datavalidator.Validator"
        logger.info("Check VALIDATOR")
    elif service == "FDI":
        sv = "com.ligadata.flare.fdi.FdiMain"
        logger.info("Check FDI")
    else:
        return
    status_cmd = commands['status']%{"service": sv}
    service_status = con.runCommand(ssh_client, status_cmd)
    if service_status:
        return True
    else:
        return False

###########NodeIpAddr|isleader|installation_status|fdi_status|grid_status|kamanja_status|validator_status|real_path|new_path
def audit(row):
    logger.info(row)
    audit_file = '/audits/audits-{0}.csv'.format(date)
    with open('audits/audits-{0}.csv'.format(date), 'a+') as csvfile:
        filewriter = csv.writer(csvfile,delimiter="|")
        filewriter.writerow(row)
        csvfile.close()

#set the pathes on the Meta and ClusterConfig
def setPrams(node):
    Install_Path = flarePaths["Install_Path"].replace('/','\\/')
    sedClusterConf = "sed -i -E 's/\"ClusterId\":.*\"/\"ClusterId\": \"{0}\"/g; s/\"NodeId\":.*\"/\"NodeId\": \"{1}\"/g; s/\"NodeIpAddr\":.*\"/\"NodeIpAddr\": \"{2}\"/g; s/\"TenantId\":.*\"/\"TenantId\": \"{3}\"/g' {4}/ClusterConfig.json".format(names["Cluster_Id"], node["NodeId"], node["NodeIpAddr"], names["Tenant_Id"], flarePaths["Working_Dir"] + "/ClusterInstall")
    logger.info("running sed ClusterConfig: "+sedClusterConf)
    runCmdLocal(sedClusterConf)
    sedMeta = "sed -i -E 's/NODEID.*/{1}/g; s/SERVICE_HOST.*/{2}/g; s/\-[0-9]+\.[0-9]+\.[0-9]//g' {0}/ClusterInstall/MetadataAPIConfig.properties".format(flarePaths["Working_Dir"], "NODEID=%s"%node['NodeId'], "SERVICE_HOST=%s"%node['NodeIpAddr'])
    runCmdLocal(sedMeta)

def getClusterConfig(nodesQueue):
    logger.info("reading Cluster Config")
    with open("{0}/{1}".format(flarePaths['ClusterConfig'], ClusterConfig)) as f:
        info = json.load(f)
    tmpL = info['Clusters'][0]['Nodes'][0].copy()
    info['Clusters'][0]['Nodes'] = []
    logger.info(tmpL)
    if nodesQueue:
        for node in nodesQueue:
            tmpL2 = tmpL.copy()
            tmpL2['NodeIpAddr'] = node["NodeIpAddr"]
            tmpL2['NodeId'] = node["NodeId"]
            logger.info(tmpL2)
            info['Clusters'][0]['Nodes'].append(tmpL2)
    else:
        info['Clusters'][0]['Nodes'] = [tmpL]
        info['Clusters'][0]['Adapters'] = []

    logger.info("writing the new node Cluster config on: {0}/ClusterConfig.json".format(flarePaths["Working_Dir"] + "/ClusterInstall"))
    output = json.dumps(info, indent=1)
    logger.info(output)
    outfile = open('{0}/ClusterConfig.json'.format(flarePaths["Working_Dir"] + "/ClusterInstall"), 'w')
    outfile.write(output)
    outfile.close()

#stop kamanja on the node
def stopNode(node, con,service):
    ssh_client = con.connect(node["NodeIpAddr"])
    hostIp =  con.runCommand(ssh_client, "hostname -i")
    hostIp = hostIp[0]
    nRange = hostIp[len(hostIp)-4:len(hostIp)+1]
    sv = ""
    if service == "KAMANJA":
        sv = "com.ligadata.KamanjaManager.KamanjaManager"
        logger.info("stop KAMANJA")
    elif service == "GRID" and int(nRange) in gridR:
        sv = "com.ligadata.Grid.GridServerInstance"
        logger.info("stop GRID")
    elif service == "VALIDATOR" and int(nRange) in validatorR:
        sv = "com.ligadata.datavalidator.Validator"
        logger.info("stop VALIDATOR")
    elif service == "FDI":
        sv = "com.ligadata.flare.fdi.FdiMain"
        logger.info("stop FDI")
    else:
        return False
    logger.info("taking %s out of kamanja" %node["NodeIpAddr"])
    for i in range(5):
        if i < 3:
            signal = 15
        else:
            signal = 9
        cmd = commands['status']%{"service": sv}
        stdout = con.runCommand(ssh_client, cmd)
        if stdout:
            con.runCommand(ssh_client, "kill -{0} {1}".format(signal, stdout[0]))
        else:
            break
        time.sleep(5)
    return True
    con.closeConnection(ssh_client, node["NodeIpAddr"])

#start kamanja on the node
def controller(node, op, ipAddr):
    if "stop" in op:
        services = ["validator","kamanja","grid"]
    else:
        services = ["grid","validator","kamanja"]

    if "fdi" == op:
        services = ["fdi"]

    logger.info("%sing kamanja service on: %s" %(op,node))
    for service in services:
        if service == "kamanja":
            startCmd = commands["controller"]%{"Working_Dir": flarePaths["Working_Dir"], "operation": op, "services": service} + " -n %s"%node
        else:
            startCmd = commands["controller"]%{"Working_Dir": flarePaths["Working_Dir"], "operation": op, "services": service} + " -n %s"%ipAddr
        status = runCmdLocal(startCmd)


def cluster_controller(node, op):
    if "stop" in op:
        services = "validator,kamanja,grid"
    else:
        services = "grid,validator,kamanja"

    if "fdi" == op:
        services = "fdi"

    logger.info("%sing kamanja service on: %s" %(op,node))
    startCmd = commands["controller"]%{"Working_Dir": flarePaths["Working_Dir"], "operation": op, "services": services}
    status = runCmdLocal(startCmd)


def fdi_controller(node):
    logger.info("starting FDI service on: %s" %(node))
    startCmd = commands["controller"]%{"Working_Dir": flarePaths["Working_Dir"], "operation": "start", "services": "fdi"}
    status = runCmdLocal(startCmd)


def prepareCluster(con, nodesQueue, isCluster):
    cmd = linkJars("{0}/lib/system/".format(flarePaths['Working_Dir']))
    runCmdLocal(cmd)

    link_new_build("{0}/ClusterInstall".format(flarePaths['Working_Dir']))
    link_new_build("{0}/ClusterInstall".format(flarePaths['Install_Path']))

    cmd = linkJars("{0}/FDI/lib/".format(flarePaths['Working_Dir']))
    runCmdLocal(cmd)

    if isCluster:
        getClusterConfig(nodesQueue)
    else:
        getClusterConfig([])

    for node in nodesQueue:
        ssh_client = con.connect(node["NodeIpAddr"])
        cmd = linkJars("%s/lib/system"%flarePaths['Install_Path'])
        con.runCommand(ssh_client, cmd)
        cmd = linkJars("%s/FDI/lib"%flarePaths['Install_Path'])
        con.runCommand(ssh_client, cmd)
        con.closeConnection(ssh_client, node["NodeIpAddr"])

    # runCmdLocal("sed -i 's/\-{0}//g' {1}/{2}".format(versions['kamanja_old'], flarePaths['ClusterConfig'], ClusterConfig))
    runCmdLocal("sed -i -E 's/\-[0-9]+\.[0-9]+\.[0-9]\.jar/\.jar/g' {0}/{1}".format(flarePaths['ClusterConfig'], ClusterConfig))
    runCmdLocal(commands['uploadClusterConf']%{"Working_Dir": flarePaths['Working_Dir'], "ClusterConfig": "{0}/{1}".format(flarePaths['ClusterConfig'], ClusterConfig)})
    runCmdLocal("cp {1} {0}/ClusterInstall/MetadataAPIConfig.properties".format(flarePaths['Working_Dir'], flarePaths['MetadataAPIConfig_Path']))
    # sedMeta = "sed -i 's/NODEID.*/{2}/g; s/\-{1}//g' {0}/ClusterInstall/MetadataAPIConfig.properties".format(flarePaths["Working_Dir"], versions['kamanja_old'], "NODEID={NodeId}")
    sedMeta = "sed -i -E 's/NODEID.*/{1}/g; s/SERVICE_HOST.*/{2}/g; s/\-[0-9]+\.[0-9]+\.[0-9]//g' {0}/ClusterInstall/MetadataAPIConfig.properties".format(flarePaths["Working_Dir"], "NODEID={NodeId}", "SERVICE_HOST={HostName}")
    runCmdLocal(sedMeta)


def link_new_build(path):
    cmd = "ls {0} | grep -E '[0-9]+\.[0-9]+\.[0-9]+'".format(path)
    print(cmd)
    jars = runCmdLocal(cmd).split('\n')
    for jar in jars[0:len(jars)-1]:
        logger.info(jar)
        new_jar = re.sub(".[0-9]{4}", "", jar)
        logger.info("----------------------------------------"+new_jar)
        runCmdLocal("ln -s {0}/{1} {0}/{2}".format(path, jar, new_jar))


def getRange(IpRange):
    r = []
    nodeL = IpRange.split(",")
    for x in nodeL:
      arr = x.split("-")
      if(len(arr) >= 2):
        l1 = list(range(int(arr[0]),int(arr[1])+1))
        r.extend(l1)
      else:
        l2 = arr[0]
        r.append(int(l2))
    return r

def proceed(nodesQueue, audit):
    with open(audit, 'r') as f:
        for row in reversed(list(csv.reader(f))):
            if not row.split("|")[1]:
                logger.info("The Cluster is already upgraded!")
                exit()
            line = row
            break
    node_Addr = line[0].split("|")[0]
    logger.info("starting upgrade from node %s"%node_Addr)
    node = nodesQueue[0]
    for n in nodesQueue:
        if n["NodeIpAddr"] == node_Addr:
            node = n
            break
    nodesQueue = nodesQueue[nodesQueue.index(node)+1:]
    logger.info("nodes to be upgraded: %s"%nodesQueue)
    return nodesQueue


def upgrade_fdi(con):
    upgradeNode(fdi, con)
    stopNode(fdi, con, "FDI")
    fdi_controller(node)


def main(argv) :
    logger.setLogger()
    parser = argparse.ArgumentParser()
    parser.add_argument('-jf','--jsonFile', help='json file config', required=True)
    parser.add_argument('-pc', '--prepareCluster', help='prepareCluster', required=False)
    parser.add_argument('-pr', '--proceed', help='proceed from the last upgraded node, just pass the audit file', required=False)
    parser.add_argument('-uc', '--upgradeCluster', help='Upgrading the Cluster at one time', required=False)
    parser.add_argument('-rbc', '--rollbackCluster', help='Rollback the Cluster at one time', required=False)
    parser.add_argument('-rb', '--rollback', help='rollback the cluster to the previuse version, just pass the audit path', required=False)
    args = parser.parse_args()


    logger.info("config file path %s" % args.jsonFile)
    nodesQueue = readConfFile(args.jsonFile)
    logger.info("nodes :%s " %nodesQueue)

    con = SSHConnection.SSHConnection()
    global date
    global ClusterConfig
    global gridR
    global validatorR
    global soft_link
    ClusterConfig = "ClusterConfig-{0}.json".format(names["Environment"])
    gridR = getRange(ranges['grid'])
    validatorR = getRange(ranges['validator'])
    logger.info("Grid nodes range %s"%gridR)
    logger.info("Validator nodes range %s"%validatorR)
    date = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    soft_link = "Flare_{0}.{1}_{2}".format(versions['kamanja_new'], versions['build_version'], date)

    if args.rollbackCluster:
        logger.info("starting downgrade to the previuse version: %s"%args.rollbackCluster)
        rollbackCluster(con, nodesQueue, args.rollbackCluster)
        exit()

    if args.rollback:
        logger.info("starting downgrade to the previuse version: %s"%args.rollback)
        rollback(con, args.rollback)
        exit()

    if args.proceed:
        nodesQueue = proceed(nodesQueue, args.proceed)

    if args.prepareCluster:
        prepareCluster(con, nodesQueue, False)
        logger.info("Preperation step is done")
        exit()

    if not args.proceed and not args.upgradeCluster:
        leader,nodesQueue = getLeader(nodesQueue, con)
        logger.info("leader node is :%s " %leader)
        logger.info("nodes without the leader :%s " %nodesQueue)

    if args.upgradeCluster:
        upgrade_cluster(con, nodesQueue)
        exit()


    logger.info("upgrading non-leader nodes")
    audit_line = ["NodeIpAddr","isleader","installation_status","fdi_status", "grid_status", "kamanja_status", "validator_status", "real_path", "new_path"]
    audit(audit_line)
    for node in nodesQueue:
        logger.info("remaining nodes: %s"%(len(nodesQueue)+1-nodesQueue.index(node)))
        rem = []
        for i in nodesQueue[nodesQueue.index(node):len(nodesQueue)+1]:
            rem.append(i['NodeIpAddr'])
        logger.info("remaining nodes: %s"%rem)
        upgradeNode(node, con)


    logger.info("upgrading the leader node")
    upgradeNode(leader, con)

    logger.info("upgrading fdi node")
    fdi_controller(node)

    logger.info("the installation has been done successfully on the following nodes: %s"%successfullInstalation)
    logger.info("the installation has been failed on the following nodes: %s"%unsuccessfullInstalation)



if __name__=="__main__":
  main(sys.argv)
