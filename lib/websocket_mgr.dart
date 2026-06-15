import 'dart:async';

import 'package:web_socket/web_socket.dart';
import 'package:logger/logger.dart';

/// 全局日志记录器，供其他模块复用
final logger = Logger();

/// WebSocket 信令连接管理器（单例）
///
/// 职责：
/// 1. 管理与信令服务器的 WebSocket 长连接
/// 2. 通过 [StreamController] 向订阅者广播收到的消息
/// 3. 提供统一的 send/close 接口
///
/// 使用方式：
/// ```dart
/// final ws = WebsocketMgr();
/// await ws.connect('ws://server:8080');
/// ws.messages.listen((msg) { /* 处理消息 */ });
/// ws.send(jsonEncode(data));
/// ```
class WebsocketMgr {
  // ==================== 单例 ====================
  static final WebsocketMgr _instance = WebsocketMgr._internal();

  factory WebsocketMgr() {
    return _instance;
  }

  WebsocketMgr._internal();

  // ==================== 内部状态 ====================

  /// WebSocket 连接实例，未连接时为 null
  WebSocket? _ws;

  /// 消息广播控制器 —— 所有收到的文本消息都会通过此流广播
  /// 使用 broadcast 允许多个订阅者同时监听
  final StreamController<String> _messageController =
      StreamController<String>.broadcast();

  // ==================== 公共接口 ====================

  /// 接收到的信令消息流（广播流，可多端监听）
  Stream<String> get messages => _messageController.stream;

  /// 建立 WebSocket 连接
  ///
  /// [url] 信令服务器地址，例如 `ws://127.0.0.1:8080`
  ///
  /// 如果已有连接会先关闭旧连接再建立新连接
  Future<void> connect(String url) async {
    try {
      // 先关闭已有连接，避免重复连接
      await _ws?.close();

      _ws = await WebSocket.connect(Uri.parse(url));
      logger.i('WebSocket connected to $url');

      /// 监听 WebSocket 事件流
      _ws!.events.listen((event) {
        if (event is TextDataReceived) {
          logger.i('Message received: ${event.text}');
          _messageController.add(event.text); // 广播给所有订阅者
        }
      }, onError: (error) {
        logger.e('WebSocket error: $error');
        // todo:后期补全错误处理逻辑，比如重试连接等
      }, onDone: () {
        logger.w('WebSocket connection closed');
        // todo:后期补全连接关闭处理逻辑，比如自动重连等
      });
    } catch (e) {
      logger.e('Failed to connect to WebSocket: $e');
      rethrow;
    }
  }

  /// 发送文本消息到信令服务器
  ///
  /// [message] 已序列化的 JSON 字符串
  ///
  /// 若连接未建立则抛出异常并静默返回
  void send(String message) {
    try {
      if (_ws == null) {
        throw Exception('WebSocket is not connected');
      }
      _ws!.sendText(message);
      logger.i('Message sent: $message');
    } catch (e) {
      logger.e('Error sending message: $e');
      return;
    }
  }

  /// 关闭 WebSocket 连接
  void close() {
    try {
      if (_ws != null) {
        _ws!.close();
        logger.i('WebSocket connection closed');
      }
    } catch (e) {
      logger.e('Error closing WebSocket: $e');
    }
  }
}
