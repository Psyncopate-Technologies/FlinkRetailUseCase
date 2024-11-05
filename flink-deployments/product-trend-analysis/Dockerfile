FROM deltaio/delta-docker:latest

# Set the working directory
WORKDIR /opt/spark/work-dir

USER root

# Copy your notebooks to the specified directory
COPY jupyter-notebook/work-dir/ /opt/spark/work-dir/

# Install voila
RUN pip install pyspark voila matplotlib seaborn plotly

RUN chown -R NBuser:NBuser /opt/spark/work-dir

ENTRYPOINT [""]

USER NBuser

# Set the command to run when the container starts
CMD ["voila","/opt/spark/work-dir/product-trend-hourly.ipynb","--no-browser","--Voila.ip=0.0.0.0","--progressive_rendering=True","--Voila.tornado_settings", "disable_check_xsrf=true","--Voila.tornado_settings", "allow_origin=*","--VoilaConfiguration.file_allowlist", "['.*','/opt/spark/work-dir/iframe_figures/.*']"]