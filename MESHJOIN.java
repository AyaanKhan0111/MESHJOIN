package metro;

import java.io.*;
import java.sql.*;
import java.util.*;

public class MESHJOIN {
    // Connection parameters for MySQL
    private static final String DB_URL = "jdbc:mysql://localhost:3306/metro";
    private static Connection conn = null;
    
    // Buffer sizes
    private static final int DISK_BUFFER_SIZE = 10;
    private static final int STREAM_BUFFER_SIZE = 5;
    
    // Data structures for MESHJOIN
    private static List<Map<String, String>> diskBufferProducts;
    private static List<Map<String, String>> diskBufferCustomers;
    private static Queue<Map<String, String>> transactionQueue;
    private static Map<String, List<Map<String, String>>> hashTable;
    private static List<Map<String, String>> streamBuffer;
    private static List<Map<String, String>> joinedResults;
    
    public static void main(String[] args) {
        try {
            // Initialize data structures
            diskBufferProducts = new ArrayList<>();
            diskBufferCustomers = new ArrayList<>();
            transactionQueue = new LinkedList<>();
            hashTable = new HashMap<>();
            streamBuffer = new ArrayList<>();
            joinedResults = new ArrayList<>();
            
            // Connect to MySQL
            connectToDatabase();
            
            // Process the data using MESHJOIN
            processMeshJoin();
            
            // Write results to CSV
            writeResultsToDatabase();
            
            System.out.println("Processing completed successfully. Results written to combined.csv");
            
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (conn != null) conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
    
    private static void connectToDatabase() throws SQLException {
        conn = DriverManager.getConnection(DB_URL, "root", "123456");
    }
    
    private static void processMeshJoin() throws Exception {
        BufferedReader transactionReader = new BufferedReader(new FileReader("C:\\Users\\Ayaan\\Desktop\\Data Warehousing\\transactions.csv"));
        String line;
        boolean headerSkipped = false;
        
        while ((line = transactionReader.readLine()) != null) {
            if (!headerSkipped) {
                headerSkipped = true;
                continue;
            }
            
            try {
                // Process stream buffer
                if (streamBuffer.size() < STREAM_BUFFER_SIZE) {
                    Map<String, String> transaction = parseTransactionLine(line);
                    if (transaction != null) {
                        streamBuffer.add(transaction);
                    }
                    continue;
                }
                
                // Process one iteration of MESHJOIN
                processMeshJoinIteration();
                
                // Clear stream buffer and add new transaction
                streamBuffer.clear();
                Map<String, String> transaction = parseTransactionLine(line);
                if (transaction != null) {
                    streamBuffer.add(transaction);
                }
            } catch (Exception e) {
                System.err.println("Error processing line: " + line);
                e.printStackTrace();
            }
        }
        
        // Process remaining data
        if (!streamBuffer.isEmpty()) {
            processMeshJoinIteration();
        }
        
        transactionReader.close();
    }
    
    private static void processMeshJoinIteration() throws SQLException {
        // Load stream buffer into hash table
        for (Map<String, String> transaction : streamBuffer) {
            String customerId = transaction.get("customer_id");
            hashTable.computeIfAbsent(customerId, k -> new ArrayList<>()).add(transaction);
            transactionQueue.add(transaction);
        }
        
        // Process master data in partitions
        processCustomerPartitions();
        processProductPartitions();
        
        // Remove processed transactions
        for (int i = 0; i < streamBuffer.size(); i++) {
            Map<String, String> processed = transactionQueue.poll();
            if (processed != null) {
                String customerId = processed.get("customer_id");
                hashTable.get(customerId).remove(processed);
                if (hashTable.get(customerId).isEmpty()) {
                    hashTable.remove(customerId);
                }
            }
        }
    }
    
    private static void processCustomerPartitions() throws SQLException {
        String customerQuery = "SELECT * FROM customers LIMIT ? OFFSET ?";
        int offset = 0;
        
        while (true) {
            PreparedStatement pstmt = conn.prepareStatement(customerQuery);
            pstmt.setInt(1, DISK_BUFFER_SIZE);
            pstmt.setInt(2, offset);
            
            ResultSet rs = pstmt.executeQuery();
            if (!rs.next()) break;
            
            do {
                Map<String, String> customer = new HashMap<>();
                customer.put("customer_id", rs.getString("customer_id"));
                customer.put("customer_name", rs.getString("customer_name"));
                customer.put("gender", rs.getString("gender"));
                
                // Join with transactions in hash table
                String customerId = customer.get("customer_id");
                if (hashTable.containsKey(customerId)) {
                    for (Map<String, String> transaction : hashTable.get(customerId)) {
                        enrichTransaction(transaction, customer, null);
                    }
                }
            } while (rs.next());
            
            offset += DISK_BUFFER_SIZE;
            rs.close();
            pstmt.close();
        }
    }
    
    private static void processProductPartitions() throws SQLException {
        String productQuery = "SELECT * FROM products LIMIT ? OFFSET ?";
        int offset = 0;

        while (true) {
            PreparedStatement pstmt = conn.prepareStatement(productQuery);
            pstmt.setInt(1, DISK_BUFFER_SIZE);
            pstmt.setInt(2, offset);

            ResultSet rs = pstmt.executeQuery();
            if (!rs.next()) break;

            do {
                Map<String, String> product = new HashMap<>();
                product.put("productID", rs.getString("productID"));
                product.put("productName", rs.getString("productName"));
                product.put("productPrice", rs.getString("productPrice").replace("$", ""));
                product.put("supplierID", rs.getString("supplierID"));
                product.put("supplierName", rs.getString("supplierName"));
                product.put("storeID", rs.getString("storeID"));
                product.put("storeName", rs.getString("storeName"));

                // Fix for supplierName containing commas
                String supplierName = product.get("supplierName");
                if (supplierName.contains(",")) {
                    // If supplierName contains commas, ensure it isn't split
                    product.put("supplierName", supplierName.replace(",", " "));
                }

                // Join with transactions
                String productId = product.get("productID");
                for (List<Map<String, String>> transactions : hashTable.values()) {
                    for (Map<String, String> transaction : transactions) {
                        if (transaction.get("ProductID").equals(productId)) {
                            enrichTransaction(transaction, null, product);
                        }
                    }
                }
            } while (rs.next());

            offset += DISK_BUFFER_SIZE;
            rs.close();
            pstmt.close();
        }
    }

    
    private static Map<String, String> parseTransactionLine(String line) {
        try {
            // Try to handle the case where commas in supplierName might cause split issues
            // If the line contains quotes, we can assume it has escaped commas
            String[] parts = line.split(",");
            
            // If splitting doesn't result in enough parts, handle more sophisticated logic here
            if (parts.length < 6) {
                parts = line.split("\t");
            }

            // If we still don't have enough parts, log and skip
            if (parts.length < 6) {
                System.err.println("Invalid line format: " + line);
                return null;
            }

            // Checking if the supplierName contains commas and handling it accordingly
            String supplierName = parts[5].trim();
            if (supplierName.contains(",")) {
                // Assuming supplier name is the second field and should not be split
                supplierName = parts[5] + "," + parts[6];  // Concatenate parts containing commas
                parts[5] = supplierName;  // Update the supplierName field with correct value
            }

            // Create a map for transaction data
            Map<String, String> transaction = new HashMap<>();
            transaction.put("Order_ID", parts[0].trim());
            transaction.put("Order_Date", parts[1].trim());
            transaction.put("ProductID", parts[2].trim());
            transaction.put("Quantity", parts[3].trim());
            transaction.put("customer_id", parts[4].trim());
            transaction.put("time_id", parts[5].trim());

            return transaction;
        } catch (Exception e) {
            System.err.println("Error parsing line: " + line);
            e.printStackTrace();
            return null;
        }
    }

    


    private static void enrichTransaction(Map<String, String> transaction,
								          Map<String, String> customer,
								          Map<String, String> product) {
			if (customer != null) {
			transaction.put("customer_name", customer.get("customer_name"));
			transaction.put("gender", customer.get("gender"));
			}
			
			if (product != null) {
			transaction.put("productName", product.get("productName"));
			transaction.put("productPrice", product.get("productPrice"));
			transaction.put("supplierID", product.get("supplierID")); // Use storeID as supplierID
			transaction.put("supplierName", product.get("supplierName"));
			
			// Retain storeID and storeName without modification
			transaction.put("storeID", product.get("storeID"));
			transaction.put("storeName", product.get("storeName"));
			
			// Calculate total sale with error handling
			try {
			double price = Double.parseDouble(product.get("productPrice"));
			int quantity = Integer.parseInt(transaction.get("Quantity"));
			transaction.put("total_sale", String.valueOf(price * quantity));
			
			// Add to joined results if all data is present
			if (transaction.containsKey("customer_name")) {
			joinedResults.add(new HashMap<>(transaction));
			}
			} catch (NumberFormatException e) {
			System.err.println("Error calculating total sale for transaction: " + transaction);
			e.printStackTrace();
			}
			}
}
    
    private static void writeResultsToDatabase() throws SQLException {
        String insertQuery = "INSERT INTO combined (Order_ID, Order_Date, ProductID, Quantity, customer_id, time_id, " +
                "customer_name, gender, productName, productPrice, supplierID, supplierName, storeID, storeName, total_sale) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        try (PreparedStatement pstmt = conn.prepareStatement(insertQuery)) {
            for (Map<String, String> result : joinedResults) {
                pstmt.setString(1, result.getOrDefault("Order_ID", ""));
                pstmt.setString(2, result.getOrDefault("Order_Date", ""));
                pstmt.setString(3, result.getOrDefault("ProductID", ""));
                pstmt.setString(4, result.getOrDefault("Quantity", ""));
                pstmt.setString(5, result.getOrDefault("customer_id", ""));
                pstmt.setString(6, result.getOrDefault("time_id", ""));
                pstmt.setString(7, result.getOrDefault("customer_name", ""));
                pstmt.setString(8, result.getOrDefault("gender", ""));
                pstmt.setString(9, result.getOrDefault("productName", ""));
                pstmt.setString(10, result.getOrDefault("productPrice", ""));
                pstmt.setString(11, result.getOrDefault("supplierID", ""));
                pstmt.setString(12, result.getOrDefault("supplierName", ""));
                pstmt.setString(13, result.getOrDefault("storeID", ""));
                pstmt.setString(14, result.getOrDefault("storeName", ""));
                pstmt.setString(15, result.getOrDefault("total_sale", ""));
                
                pstmt.addBatch();
            }
            pstmt.executeBatch();
        }
        
        System.out.println("Results stored in the MySQL database table 'combined'.");
    }
}