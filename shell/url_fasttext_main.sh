#!/bin/bash
SPARK_HOME=/opt/spark

server_test=1
if [ "x${server_test}" = "x1" ];then
pro_home=${BASH_SOURCE-$0}
pro_home="$(dirname "${pro_home}")"
WORK_DIR=$(cd $pro_home;cd ..;pwd)
else
WORK_DIR=/tmp/duanxiping/url
echo "work dir=$WORK_DIR"

HDFS_DIR="hdfs:///tmp/duanxiping/url"
echo "hdfs dir=${HDFS_DIR}"
rm -rf ${WORK_DIR}/{shell,lib,log}
mkdir -p ${WORK_DIR}/log
hadoop fs -get ${HDFS_DIR}/lib ${WORK_DIR}
hadoop fs -get ${HDFS_DIR}/shell ${WORK_DIR}

if [ ! -e "${WORK_DIR}/title_model/all.mode.ftz" ];then
    hadoop fs -get ${HDFS_DIR}/title_model ${WORK_DIR}
fi
if [ ! -e "${WORK_DIR}/content_model/all.mode.ftz" ];then
    hadoop fs -get ${HDFS_DIR}/content_model ${WORK_DIR}
fi
fi

# construct --jars
LIB=${WORK_DIR}"/lib"
echo "lib=${LIB}"
jarfiles="${WORK_DIR}/log/jarfiles"
ls ${LIB} > ${jarfiles}
jar=""
jarlen=$(wc -l  < ${jarfiles})
echo "jarlen:${jarlen}"
if [ "x${jarlen}" != "x0" ]; then
    jar=$(awk -v wk=${LIB} 'BEGIN{a=""}{a=a?a","wk"/"$1:wk"/"$1}END{print a}' ${jarfiles})
	if [ ! -z ${jar} ]; then jar="--jars ${jar}"; fi
fi
echo "jars=$jar"

# construct --files

myjar="${WORK_DIR}/shell/url_label-1.0-SNAPSHOT.jar"
class="com.meizu.algo.browser.UrlFastText"
mysubmit(){
    echo "$@"
    submit_files=""
    num_executors=$1
    shift
    act=$1
    shift
    is_debug=$1
    shift
    stat_date=$1
    shift
    if [ "x${act}" = "xtitlep" ];then
        model_files=${WORK_DIR}/title_model/all.mode.ftz
	echo "model_files:${model_files}"
	submit_files="--files ${model_files}"
    elif [ "x${act}" = "xtitlec" ];then
	model_files=$(ls ${WORK_DIR}/title_model | sed '/all\.mode\.ftz/d' | awk -v wd=${WORK_DIR}/title_model 'BEGIN{a=""}{a=a?a","wd"/"$1:wd"/"$1}END{print a}')
	echo "model_files:${model_files}"
        submit_files="--files ${model_files}"
    elif [ "x${act}" = "xcontentp" ];then
        model_files=${WORK_DIR}/content_model/all.mode.ftz
        echo "model_files:${model_files}"
        submit_files="--files ${model_files}"
    elif [ "x${act}" = "xcontentc" ];then
        model_files=$(ls ${WORK_DIR}/content_model | sed '/all\.mode\.ftz/d' | awk -v wd=${WORK_DIR}/content_model 'BEGIN{a=""}{a=a?a","wd"/"$1:wd"/"$1}END{print a}')
	echo "model_files:${model_files}"
        submit_files="--files ${model_files}"
     fi


    ${SPARK_HOME}/bin/spark-submit \
        --verbose \
        --master  yarn \
        --deploy-mode client \
        ${submit_files} \
        --num-executors  ${num_executors} \
        --executor-cores 2 \
        --executor-memory 2G \
        --driver-memory 8G \
        --queue algo \
        ${jar} \
        --class ${class} \
            ${myjar} ${act} ${is_debug} ${stat_date} "$@"
}

stat_date=$1

# userpv
mysubmit 20 querydata false ${stat_date}

# uc cat
mysubmit 50 seg false ${stat_date}

#title
mysubmit 20 titlep false ${stat_date}
mysubmit 20 titlec false ${stat_date}

# content
mysubmit 20 contentp false ${stat_date}
mysubmit 20 contentc false ${stat_date}

# union
mysubmit 20 union false ${stat_date}
