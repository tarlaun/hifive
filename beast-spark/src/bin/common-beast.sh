#!/bin/bash
# common-beast.sh

# Define version for beast-spark package. Only used if beast libraries are not available locally
version="@project.version@"

# Initialize jars and packages list
declare -a jars
declare -a packages=("org.mortbay.jetty:jetty:@jetty.version@" "org.eclipse.jetty:jetty-servlet:9.4.48.v20220622" "org.eclipse.jetty:jetty-server:9.4.48.v20220622" "org.geotools:gt-epsg-hsql:@geotools.version@" "org.geotools:gt-coverage:@geotools.version@" "org.locationtech.jts:jts-core:@jts.version@")
declare -a exclude_packages=("org.apache.hadoop:hadoop-common" "org.apache.hadoop:hadoop-hdfs" "javax.media:jai_core")
declare -a repositories=("https://repo.osgeo.org/repository/release/")

# Initialize path to lib dir and check for existing beast-spark JAR
lib_dir="$(dirname "$0")/../lib"
beast_spark_jar_included=false

# Create the lib directory if doesn't exist
[ ! -d "$lib_dir" ] && mkdir -p "$lib_dir"

# Download jai_core if it does not exist locally
[ ! -f "${lib_dir}/jai_core-1.1.3.jar" ] && curl -o "${lib_dir}/jai_core-1.1.3.jar" "https://repo1.maven.org/maven2/javax/media/jai_core/1.1.3/jai_core-1.1.3.jar"

declare -a spark_args
# Populate jars with all jar files under ../lib/
for jar_file in "$lib_dir"/*.jar; do
  jars+=("$jar_file")
  [[ "$jar_file" =~ beast-spark-.*.jar ]] && beast_spark_jar_included=true
done

# Add beast-spark package if not included
if [ "$beast_spark_jar_included" = false ]; then
  packages+=("edu.ucr.cs.bdlab:beast-spark:$version")
fi

# Handle command line arguments
# Loop over command-line arguments
while [[ $# -gt 0 ]]; do
    key="$1"
    shift # remove the first argument from the list

    if [[ $key == "--jars" ]]; then
        jars+=("$1")
        shift # remove the value argument
    elif [[ $key == "--packages" ]]; then
        packages+=("$1")
        shift
    elif [[ $key == "--repositories" ]]; then
        repositories+=("$1")
        shift
    elif [[ $key == "--exclude-package" ]]; then
        excluded_packages+=("$1")
        shift
    elif [[ $key == --* ]]; then
        spark_args+=("$key" "$1")
        shift
    else
        program="$key"
        break # exit the loop when the first non-option argument is found
    fi
done

# Generate Spark arguments
all_args="--jars $(IFS=,; echo "${jars[*]}") --packages $(IFS=,; echo "${packages[*]}") --repositories $(IFS=,; echo "${repositories[*]}") --exclude-packages $(IFS=,; echo "${exclude_packages[*]}")"
all_args+=" ${spark_args[*]}"

export all_args