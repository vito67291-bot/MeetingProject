import 'dart:async';

import 'package:flutter/foundation.dart';

import 'webrtc_mgr.dart';

/// 聊天消息模型（UI 层展示用）
class Msg {
  /// 发送者显示名称
  final String senderName;

  /// 消息正文
  final String content;

  /// 是否由当前用户发送（用于区别左右气泡和对齐方向）
  final bool isSentBySelf;

  Msg(this.senderName, this.content, this.isSentBySelf);
}

/// 会议页面 UI 事件类型
///
/// 由 [MeetingController] 产生，向下游 [MeetingPage] 传递
enum MeetingPageUiEventType {
  /// 展示 SnackBar 提示
  showMessage,

  /// 关闭会议页面，返回上一页
  exitPage,
}

/// 会议页面 UI 事件
class MeetingPageUiEvent {
  /// 事件类型
  final MeetingPageUiEventType type;

  /// 事件描述文本
  final String message;

  /// 事件附加数据
  final Map<String, dynamic> payload;

  const MeetingPageUiEvent({
    required this.type,
    required this.message,
    this.payload = const {},
  });
}

/// 会议页面控制器
///
/// 职责：
/// 1. 作为 [WebRTCManager]（信令 + WebRTC）与 UI 之间的桥接层
/// 2. 管理聊天消息列表
/// 3. 将 Manager 的底层事件 [MeetingUiEvent] 转换为页面级事件 [MeetingPageUiEvent]
///
/// 典型流程：
/// ```dart
/// // 1. 创建 controller
/// final controller = MeetingController(selfId: 'user1', ...);
/// // 2. 初始化（注册 listener）
/// await controller.initialize();
/// // 3. 开始入会（建立信令 → 枚举设备 → 进房）
/// await controller.startMeeting(joinWithMic: true);
/// // 4. 退出
/// await controller.leaveMeeting(endMeetingIfHost: false);
/// ```
class MeetingController extends ChangeNotifier {
  /// 当前用户 ID
  final String selfId;

  /// 当前用户显示名称
  final String selfName;

  /// 会议房间号
  final String roomId;

  /// 是否为房主（创建者）
  final bool isHost;

  /// 信令服务器 WebSocket 地址
  final String signalingUrl;

  /// 是否已经进房（避免重复初始化）
  final bool alreadyJoined;

  /// WebRTC 核心管理器引用
  final WebRTCManager manager;

  MeetingController({
    required this.selfId,
    required this.selfName,
    required this.roomId,
    required this.isHost,
    required this.signalingUrl,
    required this.alreadyJoined,
    WebRTCManager? manager,
  }) : manager = manager ?? WebRTCManager();

  /// 页面级 UI 事件流广播控制器
  final StreamController<MeetingPageUiEvent> _uiEventController =
      StreamController<MeetingPageUiEvent>.broadcast();

  /// Manager 级 UI 事件的订阅句柄
  StreamSubscription<MeetingUiEvent>? _managerEventSub;

  /// 页面级 UI 事件流（供 [MeetingPage] 订阅）
  Stream<MeetingPageUiEvent> get uiEvents => _uiEventController.stream;

  /// 聊天消息列表（私有存储）
  List<Msg> messages = [];

  /// 聊天消息列表（只读外部视图）
  List<Msg> get chatMessages => List.unmodifiable(messages);

  /// 添加一条聊天消息并通知 UI 刷新
  ///
  /// 同时调用 [manager.sendMessage] 通过信令通道广播给全体参会者
  void addChatMessage({
    required String senderName,
    required String content,
    required bool isSentBySelf,
  }) {
    final trimmed = content.trim();
    if (trimmed.isEmpty) return;
    messages.add(Msg(senderName, trimmed, isSentBySelf));

    // 通过信令通道发送给房间内其他人
    manager.sendMessage(from: selfId, fromName: senderName, content: trimmed);
    notifyListeners();
  }

  /// 初始化控制器
  ///
  /// - 监听 manager 状态变化（通过 [ChangeNotifier] 的 addListener）
  /// - 订阅 manager 的 UI 事件流，将底层事件转换为页面级事件
  Future<void> initialize() async {
    manager.addListener(_relayManagerState);
    _managerEventSub = manager.uiEvents.listen(_handleManagerEvent);

    if (alreadyJoined) {
      return;
    }
  }

  /// 开始入会流程
  ///
  /// 1. 建立信令连接（WebSocket + register）
  /// 2. 根据 [joinWithMic] 决定是否预热麦克风权限
  /// 3. 加入房间
  /// 4. 若入会时选择开启麦克风，调用 toggleMicrophone
  ///
  /// [joinWithMic] 是否在进房后自动开启麦克风
  Future<void> startMeeting({required bool joinWithMic}) async {
    if (alreadyJoined) {
      return;
    }

    try {
      await manager.initializeSignaling(
        selfId: selfId,
        signalingUrl: signalingUrl,
      );

      // 入会前预热设备权限并刷新设备列表
      await manager.prepareDevicesForJoin(
        requestMicPermission: joinWithMic,
        requestCameraPermission: false,
      );

      // 发送进房信令并等待服务器 ACK
      await manager.joinRoom(roomId: roomId, isHost: isHost);

      // 用户选择入会时开启麦克风
      if (joinWithMic) {
        await manager.toggleMicrophone();
        if (!manager.isMicrophoneOn) {
          _emitUiEvent(
            const MeetingPageUiEvent(
              type: MeetingPageUiEventType.showMessage,
              message: '麦克风权限未授予或设备不可用，已静音入会',
            ),
          );
        }
      }
    } catch (e) {
      _emitUiEvent(
        MeetingPageUiEvent(
          type: MeetingPageUiEventType.showMessage,
          message: '初始化失败: $e',
        ),
      );
      rethrow;
    }
  }

  /// 离开当前会议
  ///
  /// [endMeetingIfHost] 若为 true 且当前用户是房主，则将整个房间关闭
  Future<void> leaveMeeting({required bool endMeetingIfHost}) {
    return manager.leaveRoom(endMeetingIfHost: endMeetingIfHost);
  }

  /// 将 manager 的状态变更转发给 UI（通过 [ChangeNotifier] 机制）
  void _relayManagerState() {
    notifyListeners();
  }

  /// 处理 manager 发出的底层会议事件，转换为页面级事件
  void _handleManagerEvent(MeetingUiEvent event) {
    switch (event.type) {
      case MeetingUiEventType.roomClosed:
        // 房间关闭 → 提示 + 退出页面
        _emitUiEvent(
          MeetingPageUiEvent(
            type: MeetingPageUiEventType.showMessage,
            message: event.message,
          ),
        );
        _emitUiEvent(
          const MeetingPageUiEvent(
            type: MeetingPageUiEventType.exitPage,
            message: '',
          ),
        );
        break;
      case MeetingUiEventType.joinFailed:
      case MeetingUiEventType.signalingError:
        // 进房失败 / 信令错误 → 弹出提示
        _emitUiEvent(
          MeetingPageUiEvent(
            type: MeetingPageUiEventType.showMessage,
            message: event.message,
          ),
        );
        break;
      case MeetingUiEventType.chatMessage:
        // 收到聊天消息 → 追加到本地消息列表
        messages.add(
          Msg(
            event.payload['from_name']?.toString() ??
                event.payload['from']?.toString() ??
                '未知',
            event.payload['content']?.toString() ?? '',
            false,
          ),
        );
        notifyListeners();
        break;
      case MeetingUiEventType.joinSucceeded:
      case MeetingUiEventType.reservationNotice:
        // 这些事件在 controller 层不需要额外处理，仅做静默确认
        break;
    }
  }

  /// 向 UI 事件流发射事件（仅当流未被关闭时）
  void _emitUiEvent(MeetingPageUiEvent event) {
    if (!_uiEventController.isClosed) {
      _uiEventController.add(event);
    }
  }

  @override
  void dispose() {
    _managerEventSub?.cancel();
    manager.removeListener(_relayManagerState);
    _uiEventController.close();
    super.dispose();
  }
}
