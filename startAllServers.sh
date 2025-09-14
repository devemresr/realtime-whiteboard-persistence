# # for dev purposes

# startAllServers() {
#     echo "Starting chaos test with staggered server launches..."
    
#     # Start first server immediately
#     osascript -e "tell application \"Terminal\" to do script \"cd $(pwd); echo 'Server 1 starting...'; PORT=3005 npx nodemon --exec tsx server/index.ts -- 3005\""
    
#     # Random intervals for other servers
#     for port in 3009 3006 3007 3008; do
#         delay=$((RANDOM % 15 + 2))  # 2-17 seconds
#         echo "Server $port will start in $delay seconds"
        
#         (
#             sleep $delay
#             osascript -e "tell application \"Terminal\" to do script \"cd $(pwd); echo 'Server $port joining cluster...'; PORT=$port npx nodemon --exec tsx server/index.ts -- $port\""
#         ) &
#     done
    
# }

# startAllServers