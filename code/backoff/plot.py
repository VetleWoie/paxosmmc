from matplotlib import pyplot as plt
import numpy as np

with open("throughput_request1.csv", "r") as file:
    raw = file.readlines()

datacount = -1
num_requests = []
data = []
fault_tolerance = []
for line in raw:
    if "fault_tolerance" in line:
        data.append([])
        fault_tolerance.append([])
        num_requests.append(int(line.split("_")[2]))
        datacount += 1
    else:
        data[datacount].append(float(line.split(',')[1]))
        fault_tolerance[datacount].append(int(line.split(',')[0]))


for measures, requests in zip(data, num_requests):
    x = np.arange(len(measures))
    plt.plot(x, measures, label=f"{requests} requests")

plt.xlabel("Fault tolerance of network")
plt.ylabel("Seconds before agreement")
plt.legend()
plt.savefig(f"plot_r{num_requests[0]}-{num_requests[-1]}_f{fault_tolerance[0][0]}-{fault_tolerance[0][-1]}.png")
plt.savefig(f"plot_r{num_requests[0]}-{num_requests[-1]}_f{fault_tolerance[0][0]}-{fault_tolerance[0][-1]}.pdf")
plt.show()
