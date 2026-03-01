#!/bin/bash
# common-beast.sh

# Define version for beast-spark package. Only used if beast libraries are not available locally
version="0.10.1-SNAPSHOT"

# Initialize jars and packages list
declare -a jars=()
declare -a packages=("org.mortbay.jetty:jetty:6.1.26" "org.eclipse.jetty:jetty-servlet:9.4.48.v20220622" "org.eclipse.jetty:jetty-server:9.4.48.v20220622" "org.locationtech.jts:jts-core:1.18.2")
declare -a exclude_packages=("org.apache.hadoop:hadoop-common" "org.apache.hadoop:hadoop-hdfs" "javax.media:jai_core")
declare -a repositories=("https://repo.osgeo.org/repository/release/")

# Initialize path to lib dir and check for existing beast-spark JAR
lib_dir="$(cd "$(dirname "$0")/../lib" && pwd)"
beast_spark_jar_included=false

# Create the lib directory if doesn't exist
[ ! -d "$lib_dir" ] && mkdir -p "$lib_dir"

# Download jai_core if it does not exist locally
if [ ! -f "${lib_dir}/jai_core-1.1.3.jar" ]; then
  curl -L -o "${lib_dir}/jai_core-1.1.3.jar" "https://repo1.maven.org/maven2/javax/media/jai_core/1.1.3/jai_core-1.1.3.jar"
fi

declare -a spark_args=()

# Populate jars with all jar files under ../lib/ (ONLY regular files)
shopt -s nullglob
for jar_file in "$lib_dir"/*.jar; do
  [ -f "$jar_file" ] || continue
  jars+=("$jar_file")
  [[ "$(basename "$jar_file")" =~ ^beast-spark-.*\.jar$ ]] && beast_spark_jar_included=true
done
shopt -u nullglob

# Add beast-spark package if not included
if [ "$beast_spark_jar_included" = false ]; then
  packages+=("edu.ucr.cs.bdlab:beast-spark:$version")
fi

# -------------------------
# Handle command line arguments
# -------------------------
while [[ $# -gt 0 ]]; do
  key="$1"
  shift

  if [[ $key == "--jars" ]]; then
    jars+=("$1")
    shift
  elif [[ $key == "--packages" ]]; then
    packages+=("$1")
    shift
  elif [[ $key == "--repositories" ]]; then
    repositories+=("$1")
    shift
  elif [[ $key == "--exclude-package" ]]; then
    # FIX: was excluded_packages (typo)
    exclude_packages+=("$1")
    shift
  elif [[ $key == --* ]]; then
    # pass-through spark arg with a value
    spark_args+=("$key" "$1")
    shift
  else
    program="$key"
    break
  fi
done


# -------------------------------------------------
# Gurobi setup (Spark Standalone-safe)
#   - Put gurobi.jar into the ONE true --jars list (absolute path exists now)
#   - Ship native libs via --archives
#   - Set LD_LIBRARY_PATH to extracted archive path (relative)
# -------------------------------------------------

# 1) gurobi.jar MUST be a real local file when spark-submit starts
GUROBI_JAR_ABS="/home/tbaha001/gurobi1103/linux64/lib/gurobi.jar"
if [ -f "$GUROBI_JAR_ABS" ]; then
  jars+=("$GUROBI_JAR_ABS")
else
  echo "WARNING: Gurobi jar not found at $GUROBI_JAR_ABS (will likely fail GRBEnv)"
fi

# 2) Native libs shipped via archive (so executors don't need /home mounted)
# NOTE: Keep #gurobi alias, so runtime appears under ./gurobi/...
spark_args+=("--archives" "hdfs:///user/tbaha001/gurobi/gurobi11.0.3_linux64.tar#gurobi")

# 3) Paths INSIDE the extracted archive (relative to executor/driver working dir)
# For v11.0.3 the top folder is typically gurobi1103/
GUROBI_TOP="gurobi1103"
GUROBI_REL="./gurobi/${GUROBI_TOP}/linux64"
GUROBI_LIB_REL="${GUROBI_REL}/lib"

# 4) Native loader path (this fixes: libgurobi110.so => not found)
spark_args+=("--conf" "spark.executorEnv.GUROBI_HOME=${GUROBI_REL}")
spark_args+=("--conf" "spark.executorEnv.LD_LIBRARY_PATH=${GUROBI_LIB_REL}:\$LD_LIBRARY_PATH")
spark_args+=("--conf" "spark.driverEnv.GUROBI_HOME=${GUROBI_REL}")
spark_args+=("--conf" "spark.driverEnv.LD_LIBRARY_PATH=${GUROBI_LIB_REL}:\$LD_LIBRARY_PATH")

# Extra safety: java.library.path as well
spark_args+=("--conf" "spark.executor.extraJavaOptions=-Djava.library.path=${GUROBI_LIB_REL}")
spark_args+=("--conf" "spark.driver.extraJavaOptions=-Djava.library.path=${GUROBI_LIB_REL}")

# -------------------------------------------------
# Gurobi license (HDFS -> localized)
# -------------------------------------------------
spark_args+=("--files" "hdfs:///user/tbaha001/licenses/gurobi.lic#gurobi.lic")
spark_args+=("--conf" "spark.executorEnv.GRB_LICENSE_FILE=gurobi.lic")
spark_args+=("--conf" "spark.driverEnv.GRB_LICENSE_FILE=gurobi.lic")

# -------------------------
# Generate Spark arguments (build AFTER all spark_args are appended)
# -------------------------
all_args="--jars $(IFS=,; echo "${jars[*]}")"
all_args+=" --packages $(IFS=,; echo "${packages[*]}")"
all_args+=" --repositories $(IFS=,; echo "${repositories[*]}")"
all_args+=" --exclude-packages $(IFS=,; echo "${exclude_packages[*]}")"
all_args+=" ${spark_args[*]}"


export all_args