import sys
import csv


def csvRows(filename):
    with open(filename, 'r') as fi:
        reader = csv.DictReader(fi)
        for row in reader:
            yield row["Product ID"], row["Customer ID"], float(row["Item Cost"])


if __name__ == '__main__':
    ProductDict = {}
    for ProductID, CustomerID, Revenue in csvRows(sys.argv[1]):
        if ProductID not in ProductDict:
            ProductDict[ProductID] = [[CustomerID], Revenue]
        else:
            if CustomerID not in ProductDict[ProductID][0]:
                ProductDict[ProductID][0].append(CustomerID)
            ProductDict[ProductID][1] += Revenue

    with open(sys.argv[2], 'w') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Product ID', 'Customer Count', 'Total Revenue'])
        for transaction in sorted(ProductDict.items(), key=lambda e: e[0]):
            writer.writerow((transaction[0], len(transaction[1][0]), transaction[1][1]))
