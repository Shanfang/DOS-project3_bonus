defmodule App do
    def main(args) do
        nodes = Enum.at(args, 0)
        num_nodes = String.to_integer(nodes)   
        requests = Enum.at(args, 1)
        num_requests = String.to_integer(requests)
        failure_percent = Enum.at(args, 3)
        failure_num = String.to_integer(failure_percent) * num_nodes / 100

        loop(num_nodes, num_requests, failure_num, 1)
    end

    def loop(num_nodes, num_requests, failure_num, n) when n > 0 do            
        Coordinator.start_link
        IO.puts "Coordinator is started..." 
        Coordinator.build_network(:coordinator, num_nodes, num_requests, failure_num)
        
        # start failure mode 30s after initing workers
        IO.puts "Mock failure, number of node will randomly be failed: "
        IO.puts failure_num
        Process.send_after(:coordinator, {:mock_failure, failure_num}, 3000)        
        
        loop(num_nodes, num_requests, failure_num, n - 1)
    end

    def loop(num_nodes, num_requests, failure_num, n) do
        :timer.sleep 1000
        loop(num_nodes, num_requests, failure_num, n)
    end
end
