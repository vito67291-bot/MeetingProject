// models/peer_models.dart
import 'package:flutter_webrtc/flutter_webrtc.dart' as webrtc;


/// 远端参会者模型
///
/// 每个 [RemotePeer] 代表房间中除自己外的一个参会者，
/// 管理其 WebRTC 连接、视频渲染器、媒体状态和 ICE 候选缓冲区
class RemotePeer {
  /// 远端用户唯一标识（与服务器注册的 session_id 一致）
  final String id;

  /// 远端用户显示名称
  String name;

  /// 与此用户的 WebRTC PeerConnection
  webrtc.RTCPeerConnection? connection;

  /// 视频渲染器 —— 将此对象传给 [webrtc.RTCVideoView] 即可显示远端画面
  webrtc.RTCVideoRenderer? renderer;

  /// 远端是否开启了视频（含屏幕共享）
  bool isVideoOn = false;

  /// 远端是否开启了音频
  bool isAudioOn = false;

  /// ICE 候选缓冲区 —— 在远端 SDP 描述尚未设置时，
  /// 收到的 ICE candidates 暂存于此，等 remoteDescription 就绪后批量添加
  final List<webrtc.RTCIceCandidate> iceBuffer = [];

  /// 当前连接状态
  PeerConnectionState state = PeerConnectionState.idle;

  RemotePeer({required this.id, this.name = ''});

  /// 释放资源：停止渲染、关闭连接
  void dispose() {
    renderer?.srcObject = null;
    renderer?.dispose();
    connection?.close();
  }
}

/// WebRTC PeerConnection 生命周期状态
enum PeerConnectionState {
  /// 初始空闲，尚未建立连接
  idle,

  /// 正在协商 SDP / 收集 ICE
  connecting,

  /// 连接成功，可收发媒体流
  connected,

  /// 连接暂时中断（网络波动）
  disconnected,

  /// 连接失败，需要重新建立
  failed,
}

/// 本地媒体状态快照
///
/// 封装摄像头/麦克风开关状态、媒体流和渲染器，
/// 用于 UI 层获取当前本地设备状态
class LocalMediaState {
  /// 摄像头是否开启
  final bool isCameraOn;

  /// 麦克风是否开启
  final bool isMicrophoneOn;

  /// 本地媒体流（包含音/视频轨道）
  final webrtc.MediaStream? stream;

  /// 本地视频渲染器（将本地画面显示在 UI 上）
  final webrtc.RTCVideoRenderer renderer;

  const LocalMediaState({
    required this.isCameraOn,
    required this.isMicrophoneOn,
    required this.stream,
    required this.renderer,
  });

  /// 按需复制并修改部分字段
  LocalMediaState copyWith({
    bool? isCameraOn,
    bool? isMicrophoneOn,
    webrtc.MediaStream? stream,
    webrtc.RTCVideoRenderer? renderer,
  }) {
    return LocalMediaState(
      isCameraOn: isCameraOn ?? this.isCameraOn,
      isMicrophoneOn: isMicrophoneOn ?? this.isMicrophoneOn,
      stream: stream ?? this.stream,
      renderer: renderer ?? this.renderer,
    );
  }
}

/// 会议全局状态
///
/// 追踪信令连接状态、当前房间和错误信息
class MeetingState {
  /// 是否已进入房间
  final bool isInRoom;

  /// 当前房间号（离开房间时为 null）
  final String? currentRoomId;

  /// 信令连接是否已建立
  final bool isSignalingConnected;

  /// 最近的一条错误消息（null 表示无错误）
  final String? errorMessage;

  const MeetingState({
    this.isInRoom = false,
    this.currentRoomId,
    this.isSignalingConnected = false,
    this.errorMessage,
  });

  /// 按需复制并修改部分字段
  ///
  /// 传入 [errorMessage] 为 null 时表示清除错误；
  /// 省略某个字段表示保持原值不变
  MeetingState copyWith({
    bool? isInRoom,
    String? currentRoomId,
    bool? isSignalingConnected,
    String? errorMessage,
  }) {
    return MeetingState(
      isInRoom: isInRoom ?? this.isInRoom,
      currentRoomId: currentRoomId ?? this.currentRoomId,
      isSignalingConnected: isSignalingConnected ?? this.isSignalingConnected,
      errorMessage: errorMessage,
    );
  }
}
