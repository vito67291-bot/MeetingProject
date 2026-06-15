// managers/webrtc_manager.dart
// ============================================================
// 本模块是会议系统的核心——WebRTC 实时音视频通话管理器
// ============================================================
// 架构概览：
//   信令层（WebSocket） ──→ 本管理器 ──→ UI 层（ChangeNotifier 驱动）
//       │                    │
//       └── SDP/ICE/媒体状态  │
//                            └── WebRTC PeerConnection + 本地媒体流
//
// 核心流程：
//   1. initializeSignaling()   → 建立 WebSocket 连接，向信令服务器注册
//   2. joinRoom()              → 初始化本地设备 → 发信令 → 等待 ACK
//   3. 远端 peer 加入            → 创建 PeerConnection → SDP 协商 → 收发媒体流
//   4. leaveRoom()             → 通知服务器 → 清理所有连接及本地资源
// ============================================================

import 'dart:async';
import 'dart:convert';
import 'package:flutter/foundation.dart';
import 'package:flutter/services.dart';
import 'package:flutter_webrtc/flutter_webrtc.dart' as webrtc;
import 'peer_models.dart';
import 'websocket_mgr.dart';
import 'http_mgr.dart';

/// 进房操作的结果
///
/// 包含服务器确认的房间信息及角色标识
class JoinRoomResult {
  /// 服务器返回的房间号
  final String roomId;

  /// 当前用户是否为房主（创建者）
  final bool isHost;

  /// 房主的用户 ID（非房主时才有意义）
  final String? hostId;

  /// 会议类型：quick（快速会议）/ reserved（预约会议）等
  final String? meetingType;

  const JoinRoomResult({
    required this.roomId,
    required this.isHost,
    this.hostId,
    this.meetingType,
  });
}

/// 会议 UI 事件类型枚举
///
/// 由 [WebRTCManager] 产生，通过 [uiEvents] 流传递给 UI 层消费
enum MeetingUiEventType {
  /// 进房成功
  joinSucceeded,

  /// 进房失败（超时/服务器拒绝等）
  joinFailed,

  /// 房间已被关闭（房主结束会议）
  roomClosed,

  /// 收到预约通知
  reservationNotice,

  /// 信令层错误
  signalingError,

  /// 聊天消息
  chatMessage,
}

class MeetingUiEvent {
  final MeetingUiEventType type;
  final String message;
  final Map<String, dynamic> payload;

  const MeetingUiEvent({
    required this.type,
    required this.message,
    this.payload = const {},
  });
}

/// WebRTC 会议管理器
///
/// 职责：
/// 1. 管理信令连接
/// 2. 管理本地媒体流
/// 3. 管理所有远端 Peer 连接
/// 4. 处理信令消息并分发
class WebRTCManager extends ChangeNotifier {
  // ==================== 单例 ====================
  static final WebRTCManager _instance = WebRTCManager._internal();
  factory WebRTCManager() => _instance;
  WebRTCManager._internal();

  // ==================== 依赖服务 ====================
  final WebsocketMgr _ws = WebsocketMgr();

  // ==================== 本地状态 ====================

  /// 当前登录用户 ID
  String _selfId = '';

  /// 当前用户显示名称
  String _selfName = '';

  /// 本地视频渲染器 —— UI 层通过 [localRenderer] 获取本地画面
  final webrtc.RTCVideoRenderer _localRenderer = webrtc.RTCVideoRenderer();

  /// 当前本地媒体流（可能包含音/视频轨道，或屏幕流）
  webrtc.MediaStream? _localStream;

  /// 屏幕共享专用流（与 _localStream 分开管理，便于停止时恢复）
  webrtc.MediaStream? _screenStream;

  /// 开启屏幕共享前的本地流备份 —— 停止共享时用于恢复原音视频流
  webrtc.MediaStream? _preScreenShareStream;

  /// 摄像头开关状态
  bool _isCameraOn = false;

  /// 麦克风开关状态
  bool _isMicrophoneOn = false;

  /// 是否正在屏幕共享
  bool _isScreenSharing = false;

  /// 已枚举的摄像头设备列表
  List<webrtc.MediaDeviceInfo> _cameraDevices = [];

  /// 已枚举的麦克风设备列表
  List<webrtc.MediaDeviceInfo> _microphoneDevices = [];

  /// 当前选中的摄像头设备 ID（null 表示未选择）
  String? _selectedCameraId;

  /// 当前选中的麦克风设备 ID（null 或 'default' 表示系统默认）
  String? _selectedMicrophoneId;

  /// 正在离会标记 —— 防止重复执行 leaveRoom 逻辑
  bool _isLeavingRoom = false;

  /// 正在清理本地媒体标记 —— 防止并发清理导致异常
  bool _isCleaningLocalMedia = false;

  /// 每个远端 peer 的操作链（串行化同一 peer 的 SDP 协商）
  final Map<String, Future<void>> _peerOpChains = {};

  /// 每个远端 peer 是否正在创建 Offer（用于完美协商模式下的冲突检测）
  final Map<String, bool> _makingOffer = {};

  // ==================== 会议状态 ====================

  /// 当前会议状态快照（房间、信令连接、错误等）
  MeetingState _meetingState = const MeetingState();

  /// 远端参会者集合，key = peerId
  final Map<String, RemotePeer> _remotePeers = {};

  /// WebSocket 消息流的订阅句柄（用于取消监听）
  StreamSubscription<String>? _wsSubscription;

  /// 进房 ACK 等待器 —— joinRoom 发信令后等待服务器返回 create/join_room_success
  Completer<JoinRoomResult>? _joinAckCompleter;

  /// 当前正在等待进房 ACK 的房间号（用于校验返回的 ACK 是否匹配）
  String? _pendingJoinRoomId;

  /// UI 事件广播流控制器 —— 用于向 UI 层下发会议事件
  final StreamController<MeetingUiEvent> _uiEventController =
      StreamController<MeetingUiEvent>.broadcast();

  // ==================== 常量配置 ====================

  /// 默认摄像头采集分辨率约束
  static const Map<String, dynamic> _defaultVideoConstraints = {
    'width': {'ideal': 1280},
    'height': {'ideal': 720},
    'facingMode': 'user',
  };

  /// ICE 服务器配置（使用 Google 公共 STUN 服务器做 NAT 穿透）
  static const Map<String, dynamic> _iceServers = {
    'iceServers': [
      {'urls': 'stun:stun.l.google.com:19302'},
    ],
  };

  // ==================== 公共 Getter ====================
  String get selfId => _selfId;
  String get selfName => _selfName;
  webrtc.RTCVideoRenderer get localRenderer => _localRenderer;
  bool get isCameraOn => _isCameraOn;
  bool get isMicrophoneOn => _isMicrophoneOn;
  MeetingState get meetingState => _meetingState;
  bool get isScreenSharing => _isScreenSharing;
  Stream<MeetingUiEvent> get uiEvents => _uiEventController.stream;

  /// 获取所有远端参会者（不可修改的视图）
  Map<String, RemotePeer> get remotePeers => Map.unmodifiable(_remotePeers);

  /// 是否已在房间中
  bool get isInRoom => _meetingState.isInRoom;

  //设备列表
  List<webrtc.MediaDeviceInfo> get cameraDevices =>
      List.unmodifiable(_cameraDevices);
  List<webrtc.MediaDeviceInfo> get microphoneDevices =>
      List.unmodifiable(_microphoneDevices);
  String? get selectedCameraId => _selectedCameraId;
  String? get selectedMicrophoneId => _selectedMicrophoneId;

  // ==================== 生命周期管理 ====================

  /// 初始化信令连接
  ///
  /// [selfId] 当前用户唯一标识
  /// [signalingUrl] WebSocket 服务器地址
  Future<void> initializeSignaling({
    required String selfId,
    String? selfName,
    required String signalingUrl,
  }) async {
    _selfId = selfId;
    if (selfName != null && selfName.trim().isNotEmpty) {
      _selfName = selfName.trim();
    } else if (_selfName.isEmpty) {
      _selfName = selfId;
    }

    if (_meetingState.isSignalingConnected) {
      logger.w('信令已初始化，跳过');
      return;
    }

    try {
      await _ws.connect(signalingUrl);

      // 监听信令消息
      _wsSubscription = _ws.messages.listen(
        _handleSignalingMessage,
        onError: (error) {
          logger.e('WebSocket 错误: $error');
          _updateMeetingState(errorMessage: '信令连接错误: $error');
        },
        onDone: () {
          logger.w('WebSocket 连接关闭');
          _updateMeetingState(isSignalingConnected: false);
        },
      );

      // 注册身份
      _sendSignalingMessage({
        'type': 'register',
        'session_id': _selfId,
        'self_name': _selfName,
      });

      _updateMeetingState(isSignalingConnected: true);
      logger.i('✅ 信令初始化完成，用户ID: $_selfId');
    } catch (e) {
      logger.e('❌ 信令初始化失败: $e');
      _updateMeetingState(errorMessage: '初始化失败: $e');
      rethrow;
    }
  }

  /// 进入会议房间
  ///
  /// [roomId] 房间号
  /// [isHost] 是否为创建者（房主）
  Future<JoinRoomResult> joinRoom({
    required String roomId,
    bool isHost = false,
    String? meetingType,
  }) async {
    _ensureSignalingReady();

    if (_meetingState.isInRoom) {
      throw StateError('已在房间中，请先调用 leaveRoom()');
    }

    try {
      // 1. 初始化本地媒体
      // await _initializeLocalMedia();

      clearMeetingError();

      // 2. 发送进房信令
      _sendSignalingMessage({
        'type': isHost ? 'create' : 'join',
        'room': roomId,
        'from': _selfId,
        if (isHost) 'meeting_type': meetingType ?? 'quick',
      });

      final joinResult = await _waitForJoinAck(roomId: roomId);

      _updateMeetingState(
        isInRoom: true,
        currentRoomId: roomId,
        errorMessage: null,
      );

      await _localRenderer.initialize();

      await _loadMediaDevices();

      // _selectedCameraId = _cameraDevices.firstOrNull?.deviceId;
      // _selectedMicrophoneId = _microphoneDevices.firstOrNull?.deviceId;

      if (_cameraDevices.isNotEmpty) {
        _selectedCameraId = _cameraDevices.first.deviceId;
      } else {
        logger.w('未枚举到摄像头列表，视频功能将不可用');
      }

      if (_microphoneDevices.isNotEmpty) {
        _selectedMicrophoneId = _microphoneDevices.first.deviceId;
      } else {
        // Linux 兜底：即使没枚举出来，也默认使用系统音频输入
        _selectedMicrophoneId = 'default';
        logger.w('未枚举到麦克风列表，强制使用系统默认麦克风(default)');
      }

      logger.i('🚪 已进入房间: $roomId');
      logger.i('🪪 服务端确认房主身份: ${joinResult.isHost}');
      logger.i(
        '📱 可用设备 - 摄像头: ${_cameraDevices.length}, 麦克风: ${_microphoneDevices.length}',
      );
      logger.i(
        '🎥 默认摄像头: ${_selectedCameraId ?? "无"}, 🎤 默认麦克风: ${_selectedMicrophoneId ?? "无"}',
      );
      return joinResult;
    } catch (e) {
      logger.e('❌ 进入房间失败: $e');
      _updateMeetingState(isInRoom: false, currentRoomId: null);
      _emitUiEvent(
        MeetingUiEvent(
          type: MeetingUiEventType.joinFailed,
          message: '进入会议失败: $e',
        ),
      );
      await _cleanupLocalMedia();
      rethrow;
    }
  }

  /// 离开当前房间
  Future<void> leaveRoom({bool endMeetingIfHost = false}) async {
    if (!_meetingState.isInRoom || _isLeavingRoom) return;

    _isLeavingRoom = true;

    try {
      final roomId = _meetingState.currentRoomId;

      // 1. 通知服务器
      if (roomId != null) {
        _sendSignalingMessage({
          'type': 'leave',
          'from': _selfId,
          'room': roomId,
          'end_meeting': endMeetingIfHost,
        });
      }

      // 2. 清理所有远端连接
      await _cleanupAllRemotePeers();

      // 3. 清理本地媒体
      await _cleanupLocalMedia();
      _cameraDevices.clear();
      _microphoneDevices.clear();
      _selectedCameraId = null;
      _selectedMicrophoneId = null;

      _updateMeetingState(isInRoom: false, currentRoomId: null);

      logger.i('🚪 已离开房间');
    } finally {
      _isLeavingRoom = false;
    }
  }

  /// 切换麦克风状态
  Future<void> toggleMicrophone() async {
    final newState = !_isMicrophoneOn;
    final useDefaultMicrophone =
        _selectedMicrophoneId == null || _selectedMicrophoneId == 'default';

    if (newState) {
      try {
        final newStream = await webrtc.navigator.mediaDevices.getUserMedia({
          'audio': !useDefaultMicrophone
              ? {
                  'deviceId': {'exact': _selectedMicrophoneId},
                }
              : true,
          'video': false,
        });

        final newTrack = newStream.getAudioTracks().first;

        if (_localStream == null) {
          _localStream = newStream;
        } else {
          final oldTracks = _localStream!.getAudioTracks().toList();
          for (var track in oldTracks) {
            try {
              await _localStream!.removeTrack(track);
            } catch (e) {
              logger.d('忽略旧音频轨道移除报错');
            }
            track.stop();
          }
          await _localStream!.addTrack(newTrack);
        }

        if (_meetingState.isInRoom) {
          await _replaceTrackOnAllConnections(newTrack);
        }
        _isMicrophoneOn = true;
      } catch (e) {
        logger.e('开启麦克风失败: $e');
        return;
      }
    } else {
      // 1. 从所有远端连接中拔出音频轨道，触发重新协商告诉对方“我没声音了”
      for (final peer in _remotePeers.values) {
        if (peer.connection != null) {
          final senders = await peer.connection!.getSenders();
          final audioSender = senders.cast<webrtc.RTCRtpSender?>().firstWhere(
            (s) => s?.track?.kind == 'audio',
            orElse: () => null,
          );
          if (audioSender != null) {
            await peer.connection!.removeTrack(audioSender);
            await _renegotiatePeer(peer.id);
          }
        }
      }

      // 2. 清理硬件占用
      await _cleanupLocalMicrophoneStream();

      // 3. 从本地流对象中彻底剥离
      final audioTracks = _localStream?.getAudioTracks().toList() ?? [];
      for (var track in audioTracks) {
        try {
          await _localStream!.removeTrack(track);
        } catch (_) {}
      }

      _isMicrophoneOn = false;
    }

    _broadcastMediaState();
    notifyListeners();
    logger.i(
      '🎤 麦克风: ${_selectedMicrophoneId ?? "无"} - ${_isMicrophoneOn ? "开启" : "关闭"}',
    );
  }

  /// 切换摄像头状态
  Future<void> toggleCamera() async {
    if (_isScreenSharing) {
      logger.w('正在屏幕共享，无法操作摄像头');
      return;
    }

    final newState = !_isCameraOn;

    if (newState) {
      try {
        final newStream = await webrtc.navigator.mediaDevices.getUserMedia({
          'audio': false,
          'video': _selectedCameraId != null
              ? {
                  'deviceId': {'exact': _selectedCameraId},
                }
              : _defaultVideoConstraints,
        });

        final newTrack = newStream.getVideoTracks().first;

        if (_localStream == null) {
          _localStream = newStream;
        } else {
          final oldTracks = _localStream!.getVideoTracks().toList();
          for (var track in oldTracks) {
            try {
              await _localStream!.removeTrack(track);
            } catch (e) {
              logger.d('忽略旧视频轨道移除报错');
            }
            track.stop();
          }
          await _localStream!.addTrack(newTrack);
        }

        _localRenderer.srcObject = _localStream;

        if (_meetingState.isInRoom) {
          await _replaceTrackOnAllConnections(newTrack);
        }
        _isCameraOn = true;
      } catch (e) {
        logger.e('开启摄像头失败: $e');
        return;
      }
    } else {
      // 1. 从所有远端连接中拔出视频轨道
      for (final peer in _remotePeers.values) {
        if (peer.connection != null) {
          final senders = await peer.connection!.getSenders();
          final videoSender = senders.cast<webrtc.RTCRtpSender?>().firstWhere(
            (s) => s?.track?.kind == 'video',
            orElse: () => null,
          );
          if (videoSender != null) {
            await peer.connection!.removeTrack(videoSender);
            await _renegotiatePeer(peer.id);
          }
        }
      }

      await _cleanupLocalCameraStream();

      final videoTracks = _localStream?.getVideoTracks().toList() ?? [];
      for (var track in videoTracks) {
        try {
          await _localStream!.removeTrack(track);
        } catch (_) {}
      }

      _localRenderer.srcObject = null;
      _isCameraOn = false;
    }

    _broadcastMediaState();
    notifyListeners();
    logger.i(
      '📹 摄像头: ${_selectedCameraId ?? "无"} - ${_isCameraOn ? "开启" : "关闭"}',
    );
  }

  /// 动态调整摄像头分辨率
  Future<void> changeCameraQuality({
    required int width,
    required int height,
  }) async {
    if (_localStream == null) return;

    try {
      // 1. 获取新分辨率的视频轨道
      final newStream = await webrtc.navigator.mediaDevices.getUserMedia({
        'audio': false,
        'video': {
          'facingMode': 'user',
          'width': {'ideal': width},
          'height': {'ideal': height},
        },
      });

      final newTrack = newStream.getVideoTracks().firstOrNull;
      if (newTrack == null) throw Exception('无法获取新视频轨道');

      newTrack.enabled = _isCameraOn;

      // 2. 替换旧轨道
      final oldTracks = _localStream!.getVideoTracks();
      if (oldTracks.isNotEmpty) {
        final oldTrack = oldTracks.first;
        try {
          await _localStream!.removeTrack(oldTrack);
        } catch (e) {
          logger.w('移除旧轨道时发生非致命错误: $e');
        }
        await oldTrack.stop();
      }

      await _localStream!.addTrack(newTrack);

      // 3. 刷新本地渲染器
      _localRenderer.srcObject = null;
      _localRenderer.srcObject = _localStream;

      // 4. 同步给所有远端
      await _replaceTrackOnAllConnections(newTrack);

      logger.i('✅ 分辨率切换成功: ${width}x${height}');
      notifyListeners();
    } catch (e) {
      logger.e('❌ 切换分辨率失败: $e');
      rethrow;
    }
  }

  /// 调整指定连接的发送画质
  Future<void> adjustSendQuality(
    String peerId, {
    double scaleDownBy = 1.0,
    int? maxBitrate,
  }) async {
    final peer = _remotePeers[peerId];
    if (peer?.connection == null) return;

    try {
      final senders = await peer!.connection!.getSenders();
      final videoSender = senders.cast<webrtc.RTCRtpSender?>().firstWhere(
        (s) => s?.track?.kind == 'video',
        orElse: () => null,
      );

      if (videoSender == null) return;

      final params = videoSender.parameters;
      if (params.encodings?.isNotEmpty ?? false) {
        params.encodings![0].scaleResolutionDownBy = scaleDownBy;
        if (maxBitrate != null) {
          params.encodings![0].maxBitrate = maxBitrate;
        }
        await videoSender.setParameters(params);

        logger.i('📊 发送画质调整 (给 $peerId): 缩放=$scaleDownBy, 码率=$maxBitrate');
      }
    } catch (e) {
      logger.e('调整发送画质失败: $e');
    }
  }

  /// 获取远端参会者的视频状态
  bool isRemoteVideoOn(String peerId) {
    return _remotePeers[peerId]?.isVideoOn ?? false;
  }

  /// 获取远端参会者的音频状态
  bool isRemoteAudioOn(String peerId) {
    return _remotePeers[peerId]?.isAudioOn ?? false;
  }

  Future<void> toggleScreenSharing() async {
    if (_isScreenSharing) {
      // 停止屏幕共享
      await _stopScreenSharing();
    } else {
      // 开始屏幕共享
      await _startScreenSharing();
    }
  }

  /// 切换指定摄像头设备
  Future<void> switchCamera(String cameraId) async {
    if (_selectedCameraId == cameraId) {
      logger.d('已选中摄像头 $cameraId，无需切换');
      return;
    }
    _selectedCameraId = cameraId; // 无论是否开启，先记录用户偏好

    // 如果当前没开视频，或者正在屏幕共享，只记录ID不执行真实硬件切换
    if (!_isCameraOn || _isScreenSharing) return;

    try {
      final newStream = await webrtc.navigator.mediaDevices.getUserMedia({
        'audio': false,
        'video': {
          'deviceId': {'exact': cameraId},
        },
      });

      final newVideoTracks = newStream.getVideoTracks();
      if (newVideoTracks.isEmpty) throw Exception('未获取到视频轨道');
      final newVideoTrack = newVideoTracks.first;

      // 剔除旧轨道并释放
      final oldVideoTracks = _localStream?.getVideoTracks() ?? [];
      for (final track in oldVideoTracks) {
        try {
          await _localStream!.removeTrack(track);
        } catch (_) {}
        track.stop();
      }

      await _localStream!.addTrack(newVideoTrack);
      _localRenderer.srcObject = _localStream;

      // 同步给所有人
      await _replaceTrackOnAllConnections(newVideoTrack);
      notifyListeners();

      logger.i('📹 已切换至摄像头: $cameraId');
    } catch (e) {
      logger.e('切换摄像头硬件失败: $e');
    }
  }

  /// 切换指定麦克风设备
  Future<void> switchMicrophone(String microphoneId) async {
    if (_selectedMicrophoneId == microphoneId) {
      logger.d('已选中麦克风 $microphoneId，无需切换');
      return;
    }
    _selectedMicrophoneId = microphoneId;

    // 如果处于静音状态，只记录ID即可
    if (!_isMicrophoneOn) return;

    try {
      final useDefaultMicrophone = microphoneId == 'default';
      final newStream = await webrtc.navigator.mediaDevices.getUserMedia({
        'audio': useDefaultMicrophone
            ? true
            : {
                'deviceId': {'exact': microphoneId},
              },
        'video': false,
      });

      final newAudioTracks = newStream.getAudioTracks();
      if (newAudioTracks.isEmpty) throw Exception('未获取到音频轨道');
      final newAudioTrack = newAudioTracks.first;

      final oldAudioTracks = _localStream?.getAudioTracks() ?? [];
      for (final track in oldAudioTracks) {
        try {
          await _localStream!.removeTrack(track);
        } catch (_) {}
        track.stop();
      }

      await _localStream!.addTrack(newAudioTrack);

      await _replaceTrackOnAllConnections(newAudioTrack);
      notifyListeners();

      logger.i('🎤 已切换至麦克风: $microphoneId');
    } catch (e) {
      logger.e('切换麦克风硬件失败: $e');
    }
  }

  Future<void> loadDevices() async {
    await _loadMediaDevices();
  }

  /// 探测麦克风是否可用。
  ///
  /// [deviceId] 传入具体设备时会尝试按设备打开；为空或 default 时使用系统默认输入。
  Future<bool> probeMicrophoneAvailability({String? deviceId}) async {
    webrtc.MediaStream? probeStream;

    try {
      final useDefaultInput =
          deviceId == null || deviceId.isEmpty || deviceId == 'default';
      probeStream = await webrtc.navigator.mediaDevices.getUserMedia({
        'audio': useDefaultInput
            ? true
            : {
                'deviceId': {'exact': deviceId},
              },
        'video': false,
      });

      return probeStream.getAudioTracks().isNotEmpty;
    } catch (e) {
      logger.w('麦克风探测失败: $e');
      return false;
    } finally {
      if (probeStream != null) {
        for (final track in probeStream.getTracks()) {
          track.stop();
        }
        await probeStream.dispose();
      }
    }
  }

  /// 入会前预热设备权限并刷新设备列表。
  ///
  /// 仅当对应开关为 true 时才触发 getUserMedia，避免无感授权弹窗。
  Future<void> prepareDevicesForJoin({
    bool requestMicPermission = false,
    bool requestCameraPermission = false,
  }) async {
    webrtc.MediaStream? probeStream;

    try {
      if (requestMicPermission || requestCameraPermission) {
        probeStream = await webrtc.navigator.mediaDevices.getUserMedia({
          'audio': requestMicPermission,
          'video': requestCameraPermission ? _defaultVideoConstraints : false,
        });
      }
    } catch (e) {
      logger.w('媒体权限预热失败: $e');
    } finally {
      await _loadMediaDevices();
      // _selectedCameraId ??= _cameraDevices.firstOrNull?.deviceId;
      // _selectedMicrophoneId ??= _microphoneDevices.firstOrNull?.deviceId;

      if (_cameraDevices.isNotEmpty && _selectedCameraId == null) {
        _selectedCameraId = _cameraDevices.first.deviceId;
      } else if (_cameraDevices.isEmpty) {
        logger.w('未枚举到摄像头列表，视频功能将不可用');
      }

      if (_microphoneDevices.isNotEmpty && _selectedMicrophoneId == null) {
        _selectedMicrophoneId = _microphoneDevices.first.deviceId;
      } else if (_microphoneDevices.isEmpty) {
        // Linux 兜底：即使没枚举出来，也默认使用系统音频输入
        _selectedMicrophoneId = 'default';
        logger.w('未枚举到麦克风列表，强制使用系统默认麦克风(default)');
      }

      notifyListeners();

      if (probeStream != null) {
        for (final track in probeStream.getTracks()) {
          track.stop();
        }
        await probeStream.dispose();
      }
    }
  }

  Future<void> sendMessage({
    required String from,
    required String fromName,
    required String content,}) async
  {
    if(content.trim().isEmpty) return;
    if(meetingState.currentRoomId == null) return;
    _sendSignalingMessage({
      'type': 'chat',
      'from': from,
      'from_name': fromName,
      'content': content.trim(),
      'room': _meetingState.currentRoomId,
    });
  }

  void updateSelfName(String selfName) {
    final normalized = selfName.trim();
    if (normalized.isEmpty) {
      _selfName = _selfId;
      return;
    }

    _selfName = normalized;
    if (_meetingState.isSignalingConnected && _meetingState.isInRoom) {
      _broadcastMediaState();
    }
  }

  // ==================== 私有方法：本地媒体管理 ====================

  // ignore: unused_element
  Future<void> _initializeLocalMedia() async {
    // 初始化渲染器
    if (_localRenderer.textureId == null) {
      await _localRenderer.initialize();
    }

    // 清理旧流
    await _cleanupLocalMedia();

    try {
      // 获取新流
      _localStream = await webrtc.navigator.mediaDevices.getUserMedia({
        'audio': true,
        'video': _defaultVideoConstraints,
      });

      // 安全地设置轨道状态（默认关闭，等用户手动开启）
      final audioTracks = _localStream!.getAudioTracks();
      final videoTracks = _localStream!.getVideoTracks();

      if (audioTracks.isNotEmpty) {
        audioTracks.first.enabled = false;
        _isMicrophoneOn = false;
      }

      if (videoTracks.isNotEmpty) {
        videoTracks.first.enabled = false;
        _isCameraOn = false;
      }

      _localRenderer.srcObject = _localStream;
      logger.i('✅ 本地媒体初始化完成');
    } catch (e) {
      logger.e('❌ 本地媒体初始化失败: $e');
      rethrow;
    }
  }

  Future<void> _loadMediaDevices() async {
    try {
      final devices = await webrtc.navigator.mediaDevices.enumerateDevices();
      _cameraDevices = devices
          .where((d) => d.kind == 'videoinput' && d.deviceId.isNotEmpty)
          .toList();
      _microphoneDevices = devices
          .where((d) => d.kind == 'audioinput' && d.deviceId.isNotEmpty)
          .toList();

      logger.i(
        '📱 设备列表更新: ${_cameraDevices.length} 摄像头, ${_microphoneDevices.length} 麦克风',
      );
    } catch (e) {
      logger.e('获取媒体设备失败: $e');
    }

    notifyListeners();
  }

  Future<void> _cleanupLocalMedia() async {
    if (_isCleaningLocalMedia) return;

    _isCleaningLocalMedia = true;
    try {
      final local = _localStream;
      final screen = _screenStream;
      final preScreenShare = _preScreenShareStream;

      _localStream = null;
      _screenStream = null;
      _preScreenShareStream = null;

      if (local != null) {
        for (final track in local.getTracks()) {
          track.stop();
        }
        await _safeDisposeStream(local, 'local');
      }

      if (screen != null && !identical(screen, local)) {
        for (final track in screen.getTracks()) {
          track.stop();
        }
        await _safeDisposeStream(screen, 'screen');
      }

      if (preScreenShare != null &&
          !identical(preScreenShare, local) &&
          !identical(preScreenShare, screen)) {
        for (final track in preScreenShare.getTracks()) {
          track.stop();
        }
        await _safeDisposeStream(preScreenShare, 'pre-screen-share');
      }

      _localRenderer.srcObject = null;
      _isCameraOn = false;
      _isMicrophoneOn = false;
      _isScreenSharing = false;
    } finally {
      _isCleaningLocalMedia = false;
    }
  }

  Future<void> _safeDisposeStream(
    webrtc.MediaStream stream,
    String streamName,
  ) async {
    try {
      await stream.dispose();
    } on PlatformException catch (e) {
      final message = e.message ?? '';
      if (message.contains('not found')) {
        logger.w('忽略重复释放流($streamName): $message');
        return;
      }
      rethrow;
    } catch (e) {
      logger.w('释放流($streamName)失败: $e');
    }
  }

  Future<void> _cleanupLocalCameraStream() async {
    if (_localStream != null) {
      for (final track in _localStream!.getTracks()) {
        if (track.kind == 'video') {
          track.stop();
        }
      }
      _isCameraOn = false;
    }
  }

  Future<void> _cleanupLocalMicrophoneStream() async {
    if (_localStream != null) {
      for (final track in _localStream!.getTracks()) {
        if (track.kind == 'audio') {
          track.stop();
        }
      }
      _isMicrophoneOn = false;
    }
  }

  /// 开始屏幕共享
  ///
  /// 流程：
  /// 1. 通过 [desktopCapturer.getSources] 获取可共享的屏幕/窗口列表
  /// 2. 调用 [getDisplayMedia] 获取屏幕视频轨道
  /// 3. 备份共享前本地流 → 构造"屏幕视频 + 原麦克风音频"的混合流
  /// 4. 替换所有远端连接中的视频轨道
  /// 5. 监听轨道 end 事件实现自动停止
  ///
  /// 失败时自动回滚到共享前的媒体状态
  Future<void> _startScreenSharing() async {
    if (_isScreenSharing) return;

    // 备份共享前的本地流，失败时用于回滚
    final localBeforeShare = _localStream;

    try {
      if (webrtc.WebRTC.platformIsAndroid || webrtc.WebRTC.platformIsIOS) {
        // 移动端：提示用户暂不支持，或引导使用"文件选择"分享图片
        throw Exception('移动端屏幕共享需要额外配置，请先使用桌面端测试');
      }

      Map<String, dynamic> displayConstraints = {'video': true, 'audio': true};

      if (webrtc.WebRTC.platformIsDesktop) {
        final sources = await webrtc.desktopCapturer.getSources(
          types: [webrtc.SourceType.Screen],
        );

        if (sources.isEmpty) {
          throw Exception('未找到可共享的屏幕或窗口，请确认系统有可用显示源');
        }

        final screenSources = sources
            .where((s) => s.type == webrtc.SourceType.Screen)
            .toList();
        final selectedSource = screenSources.isNotEmpty
            ? screenSources.first
            : sources.first;

        displayConstraints = {
          'video': {
            'deviceId': {'exact': selectedSource.id},
            'mandatory': {'frameRate': 15.0},
          },
          // Linux/macOS/Windows 上系统音频捕获兼容性差异较大，先关闭保证视频共享稳定。
          'audio': false,
        };

        logger.i('🖥️ 选择共享源: ${selectedSource.name}(${selectedSource.id})');
      }

      _screenStream = await webrtc.navigator.mediaDevices.getDisplayMedia(
        displayConstraints,
      );

      final screenVideoTracks = _screenStream!.getVideoTracks();
      if (screenVideoTracks.isEmpty) {
        throw Exception('屏幕共享未返回视频轨道');
      }

      final previousLocal = _localStream;
      _preScreenShareStream = previousLocal; // 备份共享前本地流

      // 构造“屏幕视频 + 原麦克风音频”的本地流，保证共享时持续发声。
      webrtc.MediaStream? mixedStream;
      try {
        mixedStream = await webrtc.createLocalMediaStream(
          'screen-share-${DateTime.now().millisecondsSinceEpoch}',
        );
        await mixedStream.addTrack(screenVideoTracks.first);

        for (final audioTrack in previousLocal?.getAudioTracks() ?? const []) {
          try {
            await mixedStream.addTrack(audioTrack);
          } catch (e) {
            logger.w('混合流添加音频轨失败，继续共享视频: $e');
          }
        }

        _localStream = mixedStream;
      } catch (e) {
        logger.w('构造屏幕共享混合流失败，降级为仅屏幕视频: $e');
        if (mixedStream != null) {
          await _safeDisposeStream(mixedStream, 'mixed-local-fallback');
        }
        _localStream = _screenStream;
      }

      _localRenderer.srcObject = _localStream;

      await _replaceTrackOnAllConnections(screenVideoTracks.first);

      _isScreenSharing = true;
      _isMicrophoneOn = _localStream?.getAudioTracks().isNotEmpty ?? false;
      _isCameraOn = false;
      _broadcastMediaState();
      notifyListeners();
      logger.i('🖥️ 屏幕共享已开始');

      screenVideoTracks.first.onEnded = () {
        toggleScreenSharing();
      };
    } catch (e) {
      // 启动失败时回滚到共享前状态，确保 UI 与实际媒体一致。
      if (!_isScreenSharing) {
        _localStream = _preScreenShareStream ?? localBeforeShare;
        _localRenderer.srcObject = _localStream;
        _isCameraOn = _localStream?.getVideoTracks().isNotEmpty ?? false;
        _isMicrophoneOn = _localStream?.getAudioTracks().isNotEmpty ?? false;
        _isScreenSharing = false;
        _broadcastMediaState();
        notifyListeners();
      }

      logger.e('屏幕共享失败: $e');
      rethrow;
    }
  }

  /// 停止屏幕共享
  ///
  /// 1. 停止并释放屏幕流的所有轨道
  /// 2. 切换回共享前的本地媒体流（_preScreenShareStream）
  /// 3. 恢复之前的视频轨道到所有远端连接
  /// 4. 如果之前没有视频轨道，则从所有连接中移除视频 sender
  /// 5. 清理混合流资源
  Future<void> _stopScreenSharing() async {
    if (!_isScreenSharing) return;

    try {
      final mixedLocalStream = _localStream;
      final streamToDispose = _screenStream;
      _screenStream = null;

      if (streamToDispose != null) {
        for (final track in streamToDispose.getTracks()) {
          track.stop();
        }
        await _safeDisposeStream(streamToDispose, 'screen');
      }

      _localStream = _preScreenShareStream; // 切回共享前音视频流

      final restoreVideoTrack = _preScreenShareStream
          ?.getVideoTracks()
          .firstOrNull;
      _isCameraOn = restoreVideoTrack != null;
      _isMicrophoneOn =
          _preScreenShareStream?.getAudioTracks().isNotEmpty ?? false;

      if (restoreVideoTrack != null) {
        _localRenderer.srcObject = _localStream;
        await _replaceTrackOnAllConnections(restoreVideoTrack);
      } else {
        _localRenderer.srcObject = null;
        for (final peer in _remotePeers.values) {
          if (peer.connection == null) continue;

          final senders = await peer.connection!.getSenders();
          final videoSender = senders.cast<webrtc.RTCRtpSender?>().firstWhere(
            (s) => s?.track?.kind == 'video',
            orElse: () => null,
          );

          if (videoSender != null) {
            await peer.connection!.removeTrack(videoSender);
            await _renegotiatePeer(peer.id);
          }
        }
      }

      if (mixedLocalStream != null &&
          !identical(mixedLocalStream, streamToDispose) &&
          !identical(mixedLocalStream, _preScreenShareStream)) {
        await _safeDisposeStream(mixedLocalStream, 'mixed-local');
      }

      _preScreenShareStream = null;

      _isScreenSharing = false;
      _broadcastMediaState();
      notifyListeners();
      logger.i('🖥️ 屏幕共享已停止');
    } catch (e) {
      logger.e('停止屏幕共享失败: $e');
    }
  }
  // ==================== 私有方法：远端 Peer 管理 ====================

  Future<RemotePeer> _getOrCreateRemotePeer(String peerId, {String? peerName}) async {
    if (_remotePeers.containsKey(peerId)) {
      final existing = _remotePeers[peerId]!;
      final resolvedName = (peerName ?? '').trim();
      if (resolvedName.isNotEmpty) {
        existing.name = resolvedName;
      }
      return _remotePeers[peerId]!;
    }

    final resolvedName = (peerName ?? '').trim();
    final peer = RemotePeer(
      id: peerId,
      name: resolvedName.isEmpty ? peerId : resolvedName,
    );

    // 初始化渲染器
    final renderer = webrtc.RTCVideoRenderer();
    await renderer.initialize();
    peer.renderer = renderer;

    _remotePeers[peerId] = peer;
    notifyListeners();

    return peer;
  }

  Future<void> _createPeerConnection(RemotePeer peer) async {
    if (peer.connection != null) return;

    peer.state = PeerConnectionState.connecting;

    final pc = await webrtc.createPeerConnection(_iceServers);
    peer.connection = pc;

    // 重新协商处理
    pc.onRenegotiationNeeded = () async {
      logger.i('🔄 需要重新协商: ${peer.id}');
      await _scheduleNegotiation(peer.id, reason: 'onRenegotiationNeeded');
    };

    // ICE 候选处理
    pc.onIceCandidate = (candidate) {
      _sendSignalingMessage({
        'type': 'candidate',
        'from': _selfId,
        'to': peer.id,
        'candidate': candidate.toMap(),
      });
    };

    // 轨道接收处理
    pc.onTrack = (event) async {
      logger.i('📥 收到远端轨道 [${peer.id}]: ${event.track.kind}');

      if (event.track.kind == 'video') {
        peer.isVideoOn = true;
      } else if (event.track.kind == 'audio') {
        peer.isAudioOn = true;
      }

      if (event.streams.isNotEmpty) {
        peer.renderer?.srcObject = null;
        peer.renderer?.srcObject = event.streams.first;
      }

      notifyListeners();
    };

    // 连接状态监听
    pc.onConnectionState = (state) {
      logger.i('🔗 连接状态 [${peer.id}]: $state');
      switch (state) {
        case webrtc.RTCPeerConnectionState.RTCPeerConnectionStateConnected:
          peer.state = PeerConnectionState.connected;
          break;
        case webrtc.RTCPeerConnectionState.RTCPeerConnectionStateDisconnected:
          peer.state = PeerConnectionState.disconnected;
          break;
        case webrtc.RTCPeerConnectionState.RTCPeerConnectionStateFailed:
          peer.state = PeerConnectionState.failed;
          break;
        default:
          break;
      }
      notifyListeners();
    };

    // 添加本地轨道
    if (_localStream != null) {
      for (final track in _localStream!.getTracks()) {
        await pc.addTrack(track, _localStream!);
      }

      // 屏幕共享期间，如果当前本地流里没有音频轨，尝试从共享前流补挂麦克风轨。
      if (_isScreenSharing &&
          _isMicrophoneOn &&
          _preScreenShareStream != null) {
        final senders = await pc.getSenders();
        final hasAudioSender = senders.any((s) => s.track?.kind == 'audio');
        if (!hasAudioSender) {
          for (final audioTrack in _preScreenShareStream!.getAudioTracks()) {
            try {
              await pc.addTrack(audioTrack, _preScreenShareStream!);
            } catch (e) {
              logger.w('屏幕共享期间补挂音频轨失败: $e');
            }
          }
        }
      }
    }
  }

  Future<void> _removeRemotePeer(String peerId) async {
    final peer = _remotePeers.remove(peerId);
    if (peer != null) {
      peer.dispose();
      _makingOffer.remove(peerId);
      _peerOpChains.remove(peerId);
      logger.i('👋 移除远端用户: $peerId');
      notifyListeners();
    }
  }

  Future<void> _cleanupAllRemotePeers() async {
    for (final peer in _remotePeers.values) {
      peer.dispose();
    }
    _remotePeers.clear();
    _makingOffer.clear();
    _peerOpChains.clear();
  }

  Future<JoinRoomResult> _waitForJoinAck({
    required String roomId,
    Duration timeout = const Duration(seconds: 8),
  }) async {
    if (_joinAckCompleter != null && !_joinAckCompleter!.isCompleted) {
      _joinAckCompleter!.completeError(
        StateError('Previous join request interrupted'),
      );
    }

    final completer = Completer<JoinRoomResult>();
    _joinAckCompleter = completer;
    _pendingJoinRoomId = roomId;

    try {
      return await completer.future.timeout(timeout);
    } on TimeoutException {
      if (identical(_joinAckCompleter, completer)) {
        _joinAckCompleter = null;
        _pendingJoinRoomId = null;
      }
      throw TimeoutException(
        'Join room timeout, no response from signaling server',
      );
    }
  }

  void _resolveJoinAckSuccess(Map<String, dynamic> data) {
    final completer = _joinAckCompleter;
    if (completer == null || completer.isCompleted) {
      return;
    }

    final roomId = data['room_id']?.toString();

    if (roomId != null &&
        _pendingJoinRoomId != null &&
        roomId != _pendingJoinRoomId) {
      logger.w(
        'Ignoring join ack for unexpected room: $roomId (expect: $_pendingJoinRoomId)',
      );
      return;
    }

    final result = JoinRoomResult(
      roomId: roomId ?? (_pendingJoinRoomId ?? ''),
      isHost: data['is_host'] == true,
      hostId: data['host_id']?.toString(),
      meetingType: data['meeting_type']?.toString(),
    );

    _joinAckCompleter = null;
    _pendingJoinRoomId = null;
    completer.complete(result);
    _emitUiEvent(
      MeetingUiEvent(
        type: MeetingUiEventType.joinSucceeded,
        message: '加入会议成功',
        payload: {
          'room_id': result.roomId,
          'is_host': result.isHost,
          'host_id': result.hostId,
          'meeting_type': result.meetingType,
        },
      ),
    );
  }

  void _resolveJoinAckError(String message) {
    final completer = _joinAckCompleter;
    if (completer == null || completer.isCompleted) {
      return;
    }

    _joinAckCompleter = null;
    _pendingJoinRoomId = null;
    completer.completeError(StateError(message));
    _emitUiEvent(
      MeetingUiEvent(type: MeetingUiEventType.joinFailed, message: message),
    );
  }

  Future<void> _replaceTrackOnAllConnections(
    webrtc.MediaStreamTrack newTrack,
  ) async {
    for (final peer in _remotePeers.values) {
      if (peer.connection == null) continue;

      String? trackKind = newTrack.kind;
      if (trackKind == 'video') {
        try {
          final senders = await peer.connection!.getSenders();
          final videoSender = senders.cast<webrtc.RTCRtpSender?>().firstWhere(
            (s) => s?.track?.kind == 'video',
            orElse: () => null,
          );

          if (videoSender != null) {
            await videoSender.replaceTrack(newTrack);
          } else if (_localStream != null) {
            await peer.connection!.addTrack(newTrack, _localStream!);
            await _renegotiatePeer(peer.id);
          }
        } catch (e) {
          logger.w('替换轨道失败 [${peer.id}]: $e');
        }
      } else if (trackKind == 'audio') {
        try {
          final senders = await peer.connection!.getSenders();
          final audioSender = senders.cast<webrtc.RTCRtpSender?>().firstWhere(
            (s) => s?.track?.kind == 'audio',
            orElse: () => null,
          );

          if (audioSender != null) {
            await audioSender.replaceTrack(newTrack);
          } else if (_localStream != null) {
            await peer.connection!.addTrack(newTrack, _localStream!);
            await _renegotiatePeer(peer.id);
          }
        } catch (e) {
          logger.w('替换轨道失败 [${peer.id}]: $e');
        }
      } else {
        logger.w('未知轨道类型，无法替换: $trackKind');
      }
    }
  }

  Future<void> _renegotiatePeer(String peerId) async {
    await _scheduleNegotiation(peerId, reason: 'manual_renegotiate');
  }

  /// 调度 SDP 协商（通过串行操作链防止冲突）
  ///
  /// 1. 通过 [_runPeerOperation] 串行化同一 peer 的协商操作
  /// 2. 检查 signaling state 是否为 stable（非 stable 时跳过，等待当前协商完成）
  /// 3. 检查是否已在创建 Offer 中（防止重复发起）
  /// 4. 创建 Offer → 设置本地描述 → 发送 Offer 给对方
  Future<void> _scheduleNegotiation(
    String peerId, {
    required String reason,
  }) async {
    await _runPeerOperation(peerId, () async {
      final peer = _remotePeers[peerId];
      final pc = peer?.connection;
      if (pc == null) return;

      final stable = await _isSignalingStable(pc);
      if (!stable) {
        logger.d('跳过协商 [$peerId], signaling state 非 stable, reason=$reason');
        return;
      }

      if (_makingOffer[peerId] == true) {
        logger.d('跳过协商 [$peerId], 当前已在创建 Offer, reason=$reason');
        return;
      }

      _makingOffer[peerId] = true;
      try {
        final offer = await pc.createOffer();
        await pc.setLocalDescription(offer);
        _sendSignalingMessage({
          'type': 'offer',
          'from': _selfId,
          'to': peerId,
          'sdp': offer.sdp,
        });
        logger.i('📤 发送 Offer 给: $peerId (reason=$reason)');
      } catch (e) {
        logger.w('协商 Offer 失败 [$peerId]: $e (reason=$reason)');
      } finally {
        _makingOffer[peerId] = false;
      }
    });
  }

  /// 串行化同一 peer 的操作链
  ///
  /// 将 [operation] 追加到 peer 的操作队列末尾，确保同一 peer 的
  /// SDP 协商 / ICE 处理不会并发执行，避免信令状态机异常
  Future<void> _runPeerOperation(
    String peerId,
    Future<void> Function() operation,
  ) {
    final previous = _peerOpChains[peerId] ?? Future<void>.value();
    final next = previous.catchError((_) {}).then((_) => operation());

    _peerOpChains[peerId] = next.whenComplete(() {
      if (identical(_peerOpChains[peerId], next)) {
        _peerOpChains.remove(peerId);
      }
    });

    return _peerOpChains[peerId]!;
  }

  Future<bool> _isSignalingStable(webrtc.RTCPeerConnection pc) async {
    final signalingState = await pc.getSignalingState();
    return signalingState.toString().toLowerCase().contains('stable');
  }

  /// 判断当前用户在完美协商模式下的角色
  ///
  /// 规则：selfId 字典序大于 peerId 的一方为 polite（礼貌方），
  /// 在 Offer 冲突时 polite 方让步（rollback）
  bool _isPolitePeer(String peerId) {
    return _selfId.compareTo(peerId) > 0;
  }

  // ==================== 私有方法：信令处理 ====================

  /// 信令消息入口 —— 由 WebSocket 消息流触发
  ///
  /// 根据消息 `type` 字段路由到对应的处理器，支持的信令类型包括：
  /// - register_success / create_room_success / join_room_success → 进房流程
  /// - user_joined → 新参会者通知，触发 SDP Offer
  /// - media_state → 远端媒体状态（音/视频开关）
  /// - offer / answer / candidate → WebRTC SDP 协商三件套
  /// - user_left / leave / room_closed → 离会/房间关闭
  /// - chat → 会议聊天消息
  /// - ping → 心跳（回复 pong）
  /// - error → 服务器错误
  void _handleSignalingMessage(String message) {
    try {
      final data = jsonDecode(message) as Map<String, dynamic>;
      final type = data['type'] as String?;
      final fromId = (data['from'] ?? data['session_id'] ?? data['user_id'])
          ?.toString();
        final fromName = data['from_name']?.toString();

      logger.d('📨 收到信令: $type from: $fromId');

      switch (type) {
        case 'register_success':
          logger.i('✅ 服务器注册成功');
          break;

        case 'create_room_success':
        case 'join_room_success':
          _resolveJoinAckSuccess(data);
          break;

        case 'user_joined':
          if (fromId != null && fromId != _selfId) {
            _handleUserJoined(fromId, peerName: fromName);
          }
          break;

        case 'media_state':
          if (fromId != null) {
            _handleMediaState(fromId, data, fromName: fromName);
          }
          break;

        case 'offer':
          if (fromId != null) {
            _handleOffer(fromId, data['sdp']);
          }
          break;

        case 'answer':
          if (fromId != null) {
            _handleAnswer(fromId, data['sdp']);
          }
          break;

        case 'candidate':
          if (fromId != null) {
            _handleCandidate(fromId, data['candidate']);
          }
          break;
        case 'user_left':
        case 'leave':
          if (fromId != null) {
            _removeRemotePeer(fromId);
          }
          break;
        case 'room_closed':
          unawaited(_handleRoomClosed(data));
          break;

        case 'reservation_notice':
          _emitUiEvent(
            MeetingUiEvent(
              type: MeetingUiEventType.reservationNotice,
              message: data['event']?.toString() ?? '收到预约通知',
              payload: data,
            ),
          );
          break;
        case 'chat':
          _emitUiEvent(
            MeetingUiEvent(
              type: MeetingUiEventType.chatMessage,
              message: data['content']?.toString() ?? '',
              payload: data,
            ),
          );
          break;
        case 'ping':
          _sendSignalingMessage({
            'type': 'pong',
          });
          break;
        case 'error':
          final errMsg =
              data['message']?.toString() ?? 'Unknown signaling error';
          logger.e('服务器错误: $errMsg');
          _resolveJoinAckError(errMsg);
          _updateMeetingState(errorMessage: errMsg);
          _emitUiEvent(
            MeetingUiEvent(
              type: MeetingUiEventType.signalingError,
              message: errMsg,
              payload: data,
            ),
          );
          break;

        default:
          logger.w('未知信令类型: $type');
      }

      notifyListeners();
    } catch (e) {
      logger.e('处理信令消息失败: $e');
    }
  }

  /// 处理新用户加入事件
  ///
  /// 1. 向新用户广播自己的媒体状态（音/视频开关）
  /// 2. 创建 PeerConnection 并发起 SDP Offer（呼叫）
  Future<void> _handleUserJoined(String peerId, {String? peerName}) async {
    logger.i('👤 新用户加入: $peerId');

    _sendSignalingMessage({
      'type': 'media_state',
      'from': _selfId,
      'from_name': _selfName,
      'to': peerId,
      'videoOn': _isCameraOn || _isScreenSharing,
      'audioOn': _isMicrophoneOn,
    });

    await _initiateCall(peerId, peerName: peerName);
  }

  Future<void> _handleMediaState(
    String peerId,
    Map<String, dynamic> data,
    {String? fromName}
  ) async {
    final peer = await _getOrCreateRemotePeer(peerId, peerName: fromName);

    final resolvedName = (fromName ?? '').trim();
    if (resolvedName.isNotEmpty) {
      peer.name = resolvedName;
    }

    peer.isVideoOn = data['videoOn'] ?? false;
    peer.isAudioOn = data['audioOn'] ?? false;

    notifyListeners();
    logger.i('📢 媒体状态更新 [$peerId]: 视频=${peer.isVideoOn}, 音频=${peer.isAudioOn}');
  }

  /// 处理房间关闭事件
  ///
  /// 服务器通知房间已关闭（房主结束会议或异常关闭）。
  /// 清理所有远端连接、本地媒体和设备列表，更新状态并通知 UI
  Future<void> _handleRoomClosed(Map<String, dynamic> data) async {
    if (_isLeavingRoom) {
      logger.i('本端正在离会，忽略 room_closed 的重复清理');
      return;
    }

    final roomId = data['room_id']?.toString() ?? _meetingState.currentRoomId;
    final reason = data['reason']?.toString() ?? 'unknown';
    final meetingType = data['meeting_type']?.toString() ?? 'unknown';

    if (_meetingState.currentRoomId != null &&
        roomId != null &&
        _meetingState.currentRoomId != roomId) {
      logger.w('忽略其他房间的关闭通知: $roomId');
      return;
    }

    await _cleanupAllRemotePeers();
    await _cleanupLocalMedia();
    _cameraDevices.clear();
    _microphoneDevices.clear();
    _selectedCameraId = null;
    _selectedMicrophoneId = null;

    _updateMeetingState(
      isInRoom: false,
      currentRoomId: null,
      errorMessage: '会议已关闭（$meetingType/$reason）',
    );

    _emitUiEvent(
      MeetingUiEvent(
        type: MeetingUiEventType.roomClosed,
        message: '会议已关闭（$meetingType/$reason）',
        payload: {
          'room_id': roomId,
          'meeting_type': meetingType,
          'reason': reason,
        },
      ),
    );

    logger.w('⚠️ 房间已关闭: room=$roomId, type=$meetingType, reason=$reason');
  }

  Future<void> _initiateCall(String peerId, {String? peerName}) async {
    try {
      final peer = await _getOrCreateRemotePeer(peerId, peerName: peerName);
      await _createPeerConnection(peer);
      await _scheduleNegotiation(peerId, reason: 'initiate_call');
    } catch (e) {
      logger.e('发起呼叫失败: $e');
    }
  }

  /// 处理远端 SDP Offer（WebRTC 协商——被叫方逻辑）
  ///
  /// 采用"完美协商"（Perfect Negotiation）模式：
  /// 1. 先发送自己的媒体状态给对方
  /// 2. 检测 Offer 冲突 —— 如果双方同时发 Offer，根据 polite/impolite 角色决定谁让步
  /// 3. polite 方 rollback 本地描述，impolite 方忽略对方 Offer
  /// 4. 设置远端描述 → 创建 Answer → 设置本地描述 → 发送 Answer
  Future<void> _handleOffer(String peerId, String sdp) async {
    await _runPeerOperation(peerId, () async {
      try {
        _sendSignalingMessage({
          'type': 'media_state',
          'from': _selfId,
          'from_name': _selfName,
          'to': peerId,
          'videoOn': _isCameraOn || _isScreenSharing,
          'audioOn': _isMicrophoneOn,
        });

        final peer = await _getOrCreateRemotePeer(peerId);
        await _createPeerConnection(peer);

        final pc = peer.connection!;
        final isStable = await _isSignalingStable(pc);
        final offerCollision = (_makingOffer[peerId] == true) || !isStable;
        final polite = _isPolitePeer(peerId);

        if (offerCollision && !polite) {
          logger.w('忽略冲突 Offer [$peerId], 本端为 impolite');
          return;
        }

        if (offerCollision && polite) {
          try {
            await pc.setLocalDescription(
              webrtc.RTCSessionDescription('', 'rollback'),
            );
            logger.i('检测到 Offer 冲突 [$peerId], 已 rollback 本地描述');
          } catch (e) {
            logger.w('Offer 冲突 rollback 失败 [$peerId]: $e');
          }
        }

        await pc.setRemoteDescription(
          webrtc.RTCSessionDescription(sdp, 'offer'),
        );

        // 处理缓冲的 ICE 候选
        await _processBufferedCandidates(peer);

        final answer = await pc.createAnswer();
        await pc.setLocalDescription(answer);

        _sendSignalingMessage({
          'type': 'answer',
          'from': _selfId,
          'to': peerId,
          'sdp': answer.sdp,
        });

        logger.i('📤 发送 Answer 给: $peerId');
      } catch (e) {
        logger.e('处理 Offer 失败: $e');
      }
    });
  }

  /// 处理远端 SDP Answer（WebRTC 协商——主叫方逻辑）
  ///
  /// 将 Answer 设置为远端描述，然后处理在此之前缓冲的 ICE candidates
  Future<void> _handleAnswer(String peerId, String sdp) async {
    await _runPeerOperation(peerId, () async {
      try {
        final peer = _remotePeers[peerId];
        if (peer?.connection == null) return;

        await peer!.connection!.setRemoteDescription(
          webrtc.RTCSessionDescription(sdp, 'answer'),
        );

        await _processBufferedCandidates(peer);

        logger.i('✅ Answer 处理完成: $peerId');
      } catch (e) {
        logger.e('处理 Answer 失败: $e');
      }
    });
  }

  Future<void> _handleCandidate(
    String peerId,
    Map<String, dynamic> candidateMap,
  ) async {
    try {
      final peer = _remotePeers[peerId];
      final candidate = webrtc.RTCIceCandidate(
        candidateMap['candidate'],
        candidateMap['sdpMid'],
        candidateMap['sdpMLineIndex'],
      );

      // 如果连接还没建立，先缓冲
      if (peer?.connection == null ||
          await peer!.connection!.getRemoteDescription() == null) {
        peer!.iceBuffer.add(candidate);
        logger.d('⏳ 缓冲 ICE 候选: $peerId');
        return;
      }

      await peer.connection!.addCandidate(candidate);
      logger.d('✅ 添加 ICE 候选: $peerId');
    } catch (e) {
      logger.e('处理 ICE 候选失败: $e');
    }
  }

  Future<void> _processBufferedCandidates(RemotePeer peer) async {
    if (peer.connection == null || peer.iceBuffer.isEmpty) return;

    for (final candidate in peer.iceBuffer) {
      try {
        await peer.connection!.addCandidate(candidate);
      } catch (e) {
        logger.w('添加缓冲 ICE 候选失败: $e');
      }
    }

    peer.iceBuffer.clear();
    logger.i('🔄 处理缓冲 ICE 候选完成: ${peer.id}');
  }

  void _broadcastMediaState() {
    _sendSignalingMessage({
      'type': 'media_state',
      'from': _selfId,
      'from_name': _selfName,
      'room': _meetingState.currentRoomId,
      'videoOn': _isCameraOn || _isScreenSharing,
      'audioOn': _isMicrophoneOn,
    });
  }

  void _sendSignalingMessage(Map<String, dynamic> data) {
    data['access_token'] = HttpMgr.instance().accessToken; // 全局附加访问令牌
    _ws.send(jsonEncode(data));
  }

  void _updateMeetingState({
    bool? isInRoom,
    String? currentRoomId,
    bool? isSignalingConnected,
    String? errorMessage,
  }) {
    _meetingState = _meetingState.copyWith(
      isInRoom: isInRoom,
      currentRoomId: currentRoomId,
      isSignalingConnected: isSignalingConnected,
      errorMessage: errorMessage,
    );
    notifyListeners();
  }

  void _emitUiEvent(MeetingUiEvent event) {
    if (_uiEventController.isClosed) {
      return;
    }
    _uiEventController.add(event);
  }

  void clearMeetingError() {
    if (_meetingState.errorMessage == null) return;
    _meetingState = _meetingState.copyWith(errorMessage: null);
    notifyListeners();
  }

  void _ensureSignalingReady() {
    if (!_meetingState.isSignalingConnected) {
      throw StateError('信令未初始化，请先调用 initializeSignaling()');
    }
  }

  @override
  void dispose() {
    _wsSubscription?.cancel();
    _uiEventController.close();
    _cleanupAllRemotePeers();
    _cleanupLocalMedia();
    _localRenderer.dispose();
    super.dispose();
  }
}
