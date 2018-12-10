# Pass weather speed layer jar path as the first argument
if [ -z "$1" ]
    then
        jarPath1="./speed_layer_weather_update_cloud/target/uber-speed_layer_weather_update-0.0.1-SNAPSHOT.jar"
    else
        jarPath1="$1"
fi
# Pass crime speed layer jar path as the first argument
if [ -z "$2" ]
    then
        jarPath2="./speed_layer_crime_update_cloud/target/uber-speed_layer_crime_update-0.0.1-SNAPSHOT.jar"
    else
        jarPath2="$2"
fi

# Run the speed layers on server over yarn
# It consume yarn resources
# nohup spark-submit --master yarn --deploy-mode client --class StreamWeather $jarPath1 10.0.0.2:6667 &
# nohup spark-submit --master yarn --deploy-mode client --class StreamCrime $jarPath2 10.0.0.2:6667 &

# Run the speed layers on server over local
nohup spark-submit --master local[2] --class StreamWeather $jarPath1 10.0.0.2:6667 &
nohup spark-submit --master local[2] --class StreamCrime $jarPath2 10.0.0.2:6667 &