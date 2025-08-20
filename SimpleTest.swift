import SwiftPG

@main
struct SimpleTest {
    static func main() async {
        print("Starting simple connection test...")
        
        let conn = Connection()
        let configs = ConnectionConfigs(
            socketAddress: .hostPort(host: "localhost", port: 6451),
            username: "postgres",
            password: "postgres",
            database: "postgres",
            sslmode: .disable
        )
        
        do {
            print("Attempting to connect...")
            try await conn.connect(configs: configs)
            print("✅ Connection successful!")
            
            print("Attempting to execute a simple query...")
            try await conn.execute("SELECT 1")
            print("✅ Query successful!")
            
            try await conn.close()
            print("✅ Connection closed successfully!")
        } catch {
            print("❌ Error: \(error)")
        }
        
        print("Test completed.")
    }
}