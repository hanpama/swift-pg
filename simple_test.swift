#!/usr/bin/env swift

import Foundation
import NIO

@testable import SwiftPG

// Test the connection configuration
let testEventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)

func testConnection() async throws {
    let postgres17HostPort: ConnectionConfigs.SocketAddress
    
    if let host = ProcessInfo.processInfo.environment["POSTGRES_17_HOST"], let port = ProcessInfo.processInfo.environment["POSTGRES_17_PORT"] {
        postgres17HostPort = .hostPort(host: host, port: Int(port) ?? 6453)
    } else {
        postgres17HostPort = .hostPort(host: "localhost", port: 6453)
    }
    
    print("Attempting to connect to \(postgres17HostPort)")
    
    let conn = Connection(eventLoopGroup: testEventLoopGroup)
    
    let configs = ConnectionConfigs(
        socketAddress: postgres17HostPort,
        username: "user_scram_sha_256",
        password: "a1~!@#$%^&*()_+",
        database: "postgres",
        sslmode: .disable
    )
    
    do {
        try await conn.connect(configs: configs)
        print("✓ Connection successful!")
        
        let rows = try await conn.query("SELECT 1")
        for try await row in rows {
            let value: Int = try row.decode()
            print("✓ Query result: \(value)")
        }
    } catch {
        print("✗ Connection failed: \(error)")
    }
    
    try? testEventLoopGroup.syncShutdownGracefully()
}

// Run the test
Task {
    await testConnection()
    exit(0)
}

RunLoop.main.run()