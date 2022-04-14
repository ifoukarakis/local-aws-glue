# Dockerfile extends AWS glue base image, addressing issues in execution
FROM amazon/aws-glue-libs:glue_libs_2.0.0_image_01

# Check https://stackoverflow.com/questions/70491686/aws-glue-3-0-container-not-working-for-jupyter-notebook-local-development
ENV DISABLE_SSL=true

# See https://github.com/awslabs/aws-glue-libs/issues/25#issuecomment-532045819
RUN rm /home/glue_user/spark/jars/netty-3.9.9.Final.jar

RUN rm /home/glue_user/aws-glue-libs/jars/netty-3.6.2.Final.jar \
       /home/glue_user/aws-glue-libs/jars/netty-buffer-4.1.22.Final.jar \
       /home/glue_user/aws-glue-libs/jars/netty-codec-4.1.53.Final.jar \
       /home/glue_user/aws-glue-libs/jars/netty-codec-http2-4.1.53.Final.jar \
       /home/glue_user/aws-glue-libs/jars/netty-codec-http-4.1.53.Final.jar \
       /home/glue_user/aws-glue-libs/jars/netty-common-4.1.22.Final.jar \
       /home/glue_user/aws-glue-libs/jars/netty-handler-4.1.53.Final.jar \
       /home/glue_user/aws-glue-libs/jars/netty-nio-client-2.15.32.jar \
       /home/glue_user/aws-glue-libs/jars/netty-reactive-streams-2.0.4.jar \
       /home/glue_user/aws-glue-libs/jars/netty-reactive-streams-http-2.0.4.jar \
       /home/glue_user/aws-glue-libs/jars/netty-resolver-4.1.53.Final.jar \
       /home/glue_user/aws-glue-libs/jars/netty-transport-4.1.53.Final.jar \
       /home/glue_user/aws-glue-libs/jars/netty-transport-native-epoll-4.1.53.Final-linux-x86_64.jar \
       /home/glue_user/aws-glue-libs/jars/netty-transport-native-unix-common-4.1.53.Final.jar 