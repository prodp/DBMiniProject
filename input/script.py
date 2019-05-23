
with open('lineitem_big.csv') as f:
	content = f.readlines()[:10000]

	with open('lineitem_middle.csv', "w") as f2:
		for line in content: 
			f2.write(line) 

with open('orders_big.csv') as f:
	content = f.readlines()[:10000]

	with open('orders_middle.csv', "w") as f2:
		for line in content: 
			f2.write(line) 
	
