import Foundation
import NIO
import Testing

final class PlaygroundTest {

  @Test func test() async throws {
    // let loopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    // let loop = loopGroup.next()

    // let promise: EventLoopPromise<String> = loop.makePromise()

    // promise.succeed("Hello, world!")

    // print(try await promise.futureResult.get())
    // print(try await promise.futureResult.get())
    // print(try await promise.futureResult.get())

    // 버퍼 크기가 2인 채널 생성 (버퍼가 꽉 차면 생산자와 소비자의 속도 차이에 따라 backpressure 효과를 볼 수 있음)
    let channel = Channel<Int>(bufferingPolicy: .bufferingOldest(2))

    // 소비자: 채널에서 값을 비동기적으로 읽어옴
    Task {
      for await value in channel.stream {
        print("Received: \(value)")
        // 소비자가 작업을 처리하는 데 시간이 걸린다고 가정 (0.5초)
        try await Task.sleep(nanoseconds: 500_000_000)
      }
    }

    // 생산자: 채널에 값을 보냄

    for i in 0..<10 {
      print("Sending: \(i)")
      channel.send(i)
      // 생산자는 소비자보다 빠르게 값을 생성 (0.1초 간격)
      try await Task.sleep(nanoseconds: 100_000_000)
    }
    // 모든 값을 보낸 후 채널을 닫습니다.
    channel.close()
  }
}

/// Go의 channel과 유사하게 동작하는 간단한 채널 구현
final class Channel<Element> {
  private let continuation: AsyncStream<Element>.Continuation
  let stream: AsyncStream<Element>

  /// bufferingPolicy를 통해 버퍼 크기를 조절할 수 있습니다.
  init(bufferingPolicy: AsyncStream<Element>.Continuation.BufferingPolicy = .unbounded) {
    var cont: AsyncStream<Element>.Continuation!
    self.stream = AsyncStream<Element>(bufferingPolicy: bufferingPolicy) { continuation in
      cont = continuation
    }
    self.continuation = cont
  }

  /// 값을 채널에 보냅니다.
  func send(_ element: Element) {
    continuation.yield(element)
  }

  /// 채널을 닫아 더 이상 값이 오지 않도록 합니다.
  func close() {
    continuation.finish()
  }
}
