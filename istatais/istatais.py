
def initializeEnvs(sparkLocal):
    sparkLocal.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
    sparkLocal.conf.set("spark.sql.legacy.setCommandRejectsSparkCoreConfs","false")
    sparkLocal.conf.set("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true")
    sparkLocal.conf.set("spark.executor.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true")
    sparkLocal.conf.set("spark.kryoserializer.buffer.mb","128")
    return sparkLocal
