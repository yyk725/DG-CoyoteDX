using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using WebSocketSharp;
using WebSocketSharp.Server;
using ErrorEventArgs = WebSocketSharp.ErrorEventArgs;

namespace DG_CoyoteDX
{
    public sealed class DGSocketServer
    {
        // ================= Options =================
        public sealed class Options
        {
            public string PreferredHostIp { get; set; }
            public int Port { get; set; }
            public int MessageMaxLen { get; set; }
            public Options() { PreferredHostIp = null; Port = 4567; MessageMaxLen = 1950; }
        }

        // ================= Protocol =================
        public enum Channel { A = 1, B = 2 }
        public enum StrengthOp { Decrease = 0, Increase = 1, SetTo = 2 }
        public enum WsType { heartbeat, bind, msg, @break, error }
        
        private static class ProtocolConstants
        {
            public const string MessageTargetId = "targetId";
            public const string MessageDglab = "DGLAB";
            public const string MessageBindSuccess = "200";
            public const string ChannelA = "A";
            public const string ChannelB = "B";
        }

        // ================= Events =================
        public event Action<Guid, Guid> OnBind;          // (linkId, clientId)
        public event Action<Guid, Guid> OnDisconnect;    // (linkId, clientId)
        public event Action OnHeartbeat;
        public event Action<string> OnAppMessage;

        // ================= Debug =================
        public bool DebugEnabled { get; set; } = true;
        public Action<string> DebugSink { get; set; }
        private const int DebugSampleMax = 240;
        private void D(string format, params object[] args)
        {
            var msg = string.Format("[DGLabWs {0:HH:mm:ss.fff}] ", DateTime.Now) + string.Format(format, args);
            if (DebugEnabled || DebugSink != null)
            {
                try { if (DebugSink != null) DebugSink(msg); } catch { }
                // Debug.WriteLine(msg);
            }
        }
        internal static string Trunc(string s, int max = DebugSampleMax) { if (string.IsNullOrEmpty(s)) return s; return s.Length <= max ? s : s.Substring(0, max) + "…(trunc)"; }

        // ================= Fields =================
        private readonly Options _opt;
        private readonly string _hostIp;
        private WebSocketServer _wssv;

        private sealed class Group
        {
            public readonly ConcurrentDictionary<string, bool> SessionIds = new ConcurrentDictionary<string, bool>();
        }

        // linkId -> Group
        private readonly ConcurrentDictionary<Guid, Group> _groups = new ConcurrentDictionary<Guid, Group>();
        // sessionId -> (linkId, clientId)
        private readonly ConcurrentDictionary<string, Tuple<Guid, Guid>> _sessionIndex = new ConcurrentDictionary<string, Tuple<Guid, Guid>>();
        // linkId -> "/{linkId}" (service path)
        private readonly ConcurrentDictionary<Guid, string> _paths = new ConcurrentDictionary<Guid, string>();
        // links created before Start()
        private readonly ConcurrentDictionary<Guid, bool> _pendingLinks = new ConcurrentDictionary<Guid, bool>();

        public string HostIp { get { return _hostIp; } }
        public int Port { get { return _opt.Port; } }

        public DGSocketServer(Options opt)
        {
            _opt = opt;
            _hostIp = string.IsNullOrEmpty(opt.PreferredHostIp) ? DetectIPv4() : opt.PreferredHostIp;
        }

        // ================= Start/Stop =================
        public void Start()
        {
            if (_wssv != null) return;
            _wssv = new WebSocketServer(string.Format("ws://{0}:{1}", _hostIp, _opt.Port));
            _wssv.Start();
            D("Server started at ws://{0}:{1}", _hostIp, _opt.Port);

            // Register any links created before Start
            foreach (var linkId in _pendingLinks.Keys)
                EnsureServiceForLink(linkId);
            _pendingLinks.Clear();
        }

        public void Stop()
        {
            if (_wssv == null) return;
            try { _wssv.Stop(); D("Server stopped"); }
            finally { _wssv = null; }
            _groups.Clear();
            _sessionIndex.Clear();
            _paths.Clear();
            _pendingLinks.Clear();
        }

        // ================= Link =================
        public GeneratedLink CreateLink()
        {
            var linkId = Guid.NewGuid();
            var ws = string.Format("ws://{0}:{1}/{2}", _hostIp, _opt.Port, linkId);
            var qr = string.Format("https://www.dungeon-lab.com/app-download.php#DGLAB-SOCKET#{0}", ws);
            D("CreateLink linkId={0} ws=\"{1}\"", linkId, ws);

            EnsureServiceForLink(linkId);
            return new GeneratedLink(linkId, ws, qr);
        }

        public void RemoveLink(Guid linkId)
        {
            string path;
            if (_paths.TryRemove(linkId, out path))
            {
                if (_wssv != null) _wssv.RemoveWebSocketService(path);
                D("RemoveLink linkId={0} path={1}", linkId, path);
            }
            Group dummy;
            _groups.TryRemove(linkId, out dummy);
            bool dummyPending;
            _pendingLinks.TryRemove(linkId, out dummyPending);
        }

        private void EnsureServiceForLink(Guid linkId)
        {
            var path = "/" + linkId.ToString("D");
            if (_paths.ContainsKey(linkId)) return;

            if (_wssv == null)
            {
                _pendingLinks[linkId] = true;
                D("EnsureService deferred (server not started) link={0}", linkId);
                return;
            }

            _wssv.AddWebSocketService(path, () => new AppBehavior(this, linkId));
            _paths[linkId] = path;
            bool _;
            _pendingLinks.TryRemove(linkId, out _);
            D("Service added path={0} for link={1}", path, linkId);
        }

        public sealed class GeneratedLink
        {
            public Guid LinkId { get; private set; }
            public string WsUrl { get; private set; }
            public string QrUrl { get; private set; }
            public GeneratedLink(Guid linkId, string wsUrl, string qrUrl) { LinkId = linkId; WsUrl = wsUrl; QrUrl = qrUrl; }
        }

        // ================= Server -> Clients =================
        public Task SendStrengthAsync(Guid linkId, Channel ch, StrengthOp op, int value)
        {
            if (value < 0 || value > 200) throw new ArgumentOutOfRangeException("value", "value must be [0,200]");
            var payload = string.Format("strength-{0}+{1}+{2}", (int)ch, (int)op, value);
            D("SendStrength link={0} ch={1} op={2} value={3}", linkId, ch, op, value);
            return BroadcastAsync(linkId, payload);
        }

        public Task ClearQueueAsync(Guid linkId, Channel ch)
        {
            var payload = string.Format("clear-{0}", (int)ch);
            D("ClearQueue link={0} ch={1}", linkId, ch);
            return BroadcastAsync(linkId, payload);
        }

        public Task SendPulsesAsync(Guid linkId, Channel ch, IReadOnlyList<string> hexOps)
        {
            var jsonArr = MiniJson.BuildStringArrayJson(hexOps ?? Array.Empty<string>());
            var channelLetter = ch == Channel.A ? "A" : "B";
            var payload = string.Format("pulse-{0}:{1}", channelLetter, jsonArr);
            D("SendPulses link={0} ch={1} hexOpsCount={2} payloadLen={3}", linkId, ch, hexOps == null ? 0 : hexOps.Count, payload.Length);
            return BroadcastAsync(linkId, payload);
        }

        public Task SendPulsesAsync(Guid linkId, Channel ch, PulseBuilder builder)
        {
            D("SendPulses(builder) link={0} ch={1} stepMs={2} segCount={3}", linkId, ch, builder == null ? -1 : builder.StepMs, builder == null || builder.Segments == null ? 0 : builder.Segments.Count);
            return SendPulsesAsync(linkId, ch, PulseBuilder.ToHex(builder));
        }

        public Task SendRawAsync(Guid linkId, string payload)
        {
            D("SendRaw link={0} payload=\"{1}\"", linkId, Trunc(payload));
            return BroadcastAsync(linkId, payload);
        }

        private Task BroadcastAsync(Guid linkId, string payload)
        {
            if (_wssv == null) throw new InvalidOperationException("Server not started");

            if (!_groups.TryGetValue(linkId, out var group) || group.SessionIds.IsEmpty)
            {
                D("Broadcast skipped: link={0} has no clients", linkId);
                throw new InvalidOperationException("Link has no connected clients");
            }

            if (!_paths.TryGetValue(linkId, out var servicePath))
                throw new InvalidOperationException("Service path not found for link");

            var host = _wssv.WebSocketServices[servicePath];
            if (host == null) throw new InvalidOperationException("Service host not found");

            var targets = group.SessionIds.Keys;
            return Task.Run(() =>
            {
                int ok = 0, fail = 0;
                foreach (var sessionId in targets)
                {
                    try
                    {
                        var sessionInfo = _sessionIndex[sessionId];
                        var envelope = MiniJson.BuildEnvelopeJson(
                            WsType.msg,
                            targetId: sessionInfo.Item2,    // APP ID
                            clientId: linkId,               // 第三方终端ID
                            messageRaw: payload,
                            messageIsRawJson: false
                        );
                        host.Sessions.SendTo(envelope, sessionId);
                        ok++;
                    }
                    catch
                    {
                        fail++;
                    }
                }
                D("Broadcast done link={0} ok={1} fail={2}", linkId, ok, fail);
            });
        }

        // ================= Group membership & lifecycle =================
        internal void RegisterJoin(Guid linkId, Guid clientId, string sessionId)
        {
            var grp = _groups.GetOrAdd(linkId, id => new Group());
            grp.SessionIds[sessionId] = true;
            _sessionIndex[sessionId] = Tuple.Create(linkId, clientId);
            D("Join link={0} client={1} session={2} groupSize={3}", linkId, clientId, sessionId, grp.SessionIds.Count);
            if (OnBind != null) OnBind.Invoke(linkId, clientId);
        }

        internal void Unregister(string sessionId)
        {
            Tuple<Guid, Guid> info;
            if (!_sessionIndex.TryRemove(sessionId, out info)) return;

            Group grp;
            if (_groups.TryGetValue(info.Item1, out grp))
            {
                bool dummy;
                grp.SessionIds.TryRemove(sessionId, out dummy);
                D("Leave link={0} client={1} session={2} groupSize={3}", info.Item1, info.Item2, sessionId, grp.SessionIds.Count);
            }
            if (OnDisconnect != null) OnDisconnect.Invoke(info.Item1, info.Item2);
        }

        private static string DetectIPv4()
        {
            foreach (var ni in NetworkInterface.GetAllNetworkInterfaces())
            {
                if (ni.OperationalStatus != OperationalStatus.Up) continue;
                if (ni.NetworkInterfaceType == NetworkInterfaceType.Loopback || ni.NetworkInterfaceType == NetworkInterfaceType.Tunnel) continue;
                foreach (var ua in ni.GetIPProperties().UnicastAddresses)
                {
                    if (ua.Address.AddressFamily != AddressFamily.InterNetwork) continue;
                    var ip = ua.Address;
                    if (IPAddress.IsLoopback(ip)) continue;
                    var b = ip.GetAddressBytes();
                    if (b[0] == 169 && b[1] == 254) continue; // APIPA
                    return ip.ToString();
                }
            }
            foreach (var ip in Dns.GetHostAddresses(Dns.GetHostName()))
                if (ip.AddressFamily == AddressFamily.InterNetwork && !IPAddress.IsLoopback(ip))
                    return ip.ToString();
            return "127.0.0.1";
        }

        // ================= WebSocket Behavior =================
        private sealed class AppBehavior : WebSocketBehavior
        {
            private readonly DGSocketServer _srv;
            private readonly Guid _linkId;
            private Guid _clientId;

            public AppBehavior(DGSocketServer srv, Guid linkId) { _srv = srv; _linkId = linkId; }

            protected override void OnOpen()
            {
                _clientId = Guid.NewGuid();
                var remote = Context.UserEndPoint?.ToString() ?? "unknown";
                _srv.D("Open remote={0} session={1} path=\"{2}\" -> link={3} client={4}", remote, ID, Context.RequestUri.AbsolutePath, _linkId, _clientId);
                
                _srv.RegisterJoin(_linkId, _clientId, ID);
                
                try
                {
                    var bindResponse = MiniJson.BuildBindAckJson(_clientId);
                    Send(bindResponse);
                    _srv.D("Send bind ack session={0} -> {1}", ID, DGSocketServer.Trunc(bindResponse));
                }
                catch (Exception ex)
                {
                    _srv.D("Failed to send bind ack: {0}", ex.Message);
                }
            }

            protected override void OnMessage(MessageEventArgs e)
            {
                if (!e.IsText || string.IsNullOrEmpty(e.Data)) return;                
                try
                {
                    if (MiniJson.TryParseEnvelope(e.Data, out var type, out var clientId, out var targetId, out var msgRaw))
                    {
                        _srv.D("Recv Envelope type={0} clientId={1} targetId={2} msgRaw=\"{3}\"", type, clientId, targetId, DGSocketServer.Trunc(msgRaw));
                        switch (type?.ToLowerInvariant())
                        {
                            case "heartbeat":
                                _srv.D("Heartbeat session={0} link={1} client={2}", ID, _linkId, _clientId);
                                _srv.OnHeartbeat?.Invoke();
                                break;
                                
                            case "bind":
                                var cleanMsg = msgRaw?.Trim('"');
                                if (string.Equals(cleanMsg, "DGLAB", StringComparison.OrdinalIgnoreCase) &&
                                    !string.IsNullOrEmpty(clientId) && !string.IsNullOrEmpty(targetId) &&
                                    string.Equals(clientId, _linkId.ToString("D"), StringComparison.OrdinalIgnoreCase))
                                {
                                    _srv.D("Bind request session={0} link={1} client={2} appId={3}", ID, _linkId, _clientId, clientId);
                                    
                                    try
                                    {
                                        var bindSuccess = MiniJson.BuildEnvelopeJson(
                                            WsType.bind,
                                            targetId: Guid.Parse(clientId),
                                            clientId: _linkId,
                                            messageRaw: "200",
                                            messageIsRawJson: true
                                        );
                                        Send(bindSuccess);
                                        _srv.D("Send bind success session={0} -> {1}", ID, DGSocketServer.Trunc(bindSuccess));
                                    }
                                    catch (Exception ex)
                                    {
                                        _srv.D("Failed to send bind success: {0}", ex.Message);
                                    }
                                }
                                break;
                                
                            case "msg":
                                _srv.D("AppMessage session={0} link={1} client={2} msg=\"{3}\"", ID, _linkId, _clientId, DGSocketServer.Trunc(msgRaw));
                                _srv.OnAppMessage?.Invoke(msgRaw);
                                break;
                                
                            default:
                                _srv.D("Unknown message type: {0}", type);
                                break;
                        }
                    }
                    else
                    {
                        _srv.D("Recv parse error session={0}: cannot read envelope", ID);
                    }
                }
                catch (Exception ex)
                {
                    _srv.D("Recv parse error session={0}: {1}", ID, ex.Message);
                }
            }

            protected override void OnClose(CloseEventArgs e)
            {
                _srv.D("Close session={0} code={1} reason=\"{2}\"", ID, (int)e.Code, e.Reason);
                _srv.Unregister(ID);
                base.OnClose(e);
            }

            protected override void OnError(ErrorEventArgs e)
            {
                _srv.D("Error session={0}: {1}", ID, e.Message);
                base.OnError(e);
            }
        }

        // ================= Waveform Builder & Encoder =================
        public enum PulseSegmentType { Constant, Ramp, Square }

        public sealed class PulseSegment
        {
            public PulseSegmentType Type { get; set; }
            public int Intensity { get; set; }
            public int DurationMs { get; set; }
            public int From { get; set; }
            public int To { get; set; }
            public int Intensity2 { get; set; }
            public int OnMs { get; set; }
            public int OffMs { get; set; }
            public int Repeat { get; set; }
            public PulseSegment() { Repeat = 1; }
            public static PulseSegment Const(int intensity, int durationMs) { return new PulseSegment { Type = PulseSegmentType.Constant, Intensity = intensity, DurationMs = durationMs }; }
            public static PulseSegment Ramp(int from, int to, int durationMs) { return new PulseSegment { Type = PulseSegmentType.Ramp, From = from, To = to, DurationMs = durationMs }; }
            public static PulseSegment Square(int high, int low, int onMs, int offMs, int repeat = 1) { return new PulseSegment { Type = PulseSegmentType.Square, Intensity = high, Intensity2 = low, OnMs = onMs, OffMs = offMs, Repeat = repeat }; }
        }

        public interface IPulseEncoder { string Encode(PulseSegment seg, int stepMs); }

        public sealed class V3SimpleEncoder : IPulseEncoder
        {
            private static int Clamp(int value, int min, int max) { return Math.Max(min, Math.Min(value, max)); }
            public string Encode(PulseSegment seg, int stepMs)
            {
                stepMs = Clamp(stepMs, 1, 255);
                switch (seg.Type)
                {
                    case PulseSegmentType.Constant: return EncConst(seg.Intensity, seg.DurationMs, stepMs);
                    case PulseSegmentType.Ramp: return EncRamp(seg.From, seg.To, seg.DurationMs, stepMs);
                    case PulseSegmentType.Square: return EncSquare(seg.Intensity, seg.Intensity2, seg.OnMs, seg.OffMs, seg.Repeat, stepMs);
                    default: throw new NotSupportedException();
                }
            }
            static string EncConst(int intensity, int durationMs, int stepMs)
            {
                intensity = Clamp(intensity, 0, 200);
                var step = (byte)Clamp(stepMs, 1, 255);
                var blocks = Math.Max(1, durationMs / stepMs);
                var sb = new StringBuilder(blocks * 16);
                for (int i = 0; i < blocks; i++) sb.Append(Chunk(step, step, step, step, intensity, intensity, intensity, intensity));
                return sb.ToString();
            }
            static string EncRamp(int from, int to, int durationMs, int stepMs)
            {
                from = Clamp(from, 0, 200);
                to = Clamp(to, 0, 200);
                var step = (byte)Clamp(stepMs, 1, 255);
                var blocks = Math.Max(1, durationMs / stepMs);
                var sb = new StringBuilder(blocks * 16);
                for (int i = 0; i < blocks; i++)
                {
                    var t = (double)i / Math.Max(1, blocks - 1);
                    var v = (int)Math.Round(from + t * (to - from));
                    sb.Append(Chunk(step, step, step, step, v, v, v, v));
                }
                return sb.ToString();
            }
            static string EncSquare(int high, int low, int onMs, int offMs, int repeat, int stepMs)
            {
                high = Clamp(high, 0, 200);
                low = Clamp(low, 0, 200);
                var step = (byte)Clamp(stepMs, 1, 255);
                int onB = Math.Max(1, onMs / stepMs);
                int offB = Math.Max(0, offMs / stepMs);
                var sb = new StringBuilder((onB + offB) * repeat * 16);
                for (int r = 0; r < repeat; r++)
                {
                    for (int i = 0; i < onB; i++) sb.Append(Chunk(step, step, step, step, high, high, high, high));
                    for (int i = 0; i < offB; i++) sb.Append(Chunk(step, step, step, step, low, low, low, low));
                }
                return sb.ToString();
            }
            static string Chunk(int a, int b, int c, int d, int e, int f, int g, int h) { return string.Format("{0:X2}{1:X2}{2:X2}{3:X2}{4:X2}{5:X2}{6:X2}{7:X2}", a, b, c, d, e, f, g, h); }
        }

        public sealed class PulseBuilder
        {
            public int StepMs { get; set; }
            public List<PulseSegment> Segments { get; set; }
            public IPulseEncoder Encoder { get; set; }
            public PulseBuilder() { StepMs = 10; Segments = new List<PulseSegment>(); Encoder = new V3SimpleEncoder(); }
            public PulseBuilder AddConstant(int intensity, int durationMs) { Segments.Add(PulseSegment.Const(intensity, durationMs)); return this; }
            public PulseBuilder AddRamp(int from, int to, int durationMs) { Segments.Add(PulseSegment.Ramp(from, to, durationMs)); return this; }
            public PulseBuilder AddSquare(int high, int low, int onMs, int offMs, int repeat = 1) { Segments.Add(PulseSegment.Square(high, low, onMs, offMs, repeat)); return this; }
            public static List<string> ToHex(PulseBuilder b)
            {
                var list = new List<string>();
                foreach (var seg in b.Segments)
                {
                    var big = b.Encoder.Encode(seg, b.StepMs);
                    for (int i = 0; i + 16 <= big.Length; i += 16)
                        list.Add(big.Substring(i, 16));
                }
                return list;
            }
        }

        // ===================== Minimal JSON helper (no external libs) =====================
        private static class MiniJson
        {
            public static string BuildEnvelopeJson(WsType type, Guid? targetId, Guid? clientId, string messageRaw, bool messageIsRawJson)
            {
                var sb = new StringBuilder(128 + (messageRaw?.Length ?? 0));
                sb.Append('{');

                WritePropName(sb, "type"); WriteString(sb, type.ToString()); sb.Append(',');

                WriteGuidPropIfAny(sb, "targetId", targetId);
                WriteGuidPropIfAny(sb, "clientId", clientId);

                WritePropName(sb, "message");
                if (messageIsRawJson) { sb.Append(messageRaw ?? "null"); }
                else { WriteString(sb, messageRaw ?? string.Empty); }

                sb.Append('}');
                return sb.ToString();
            }

            // 专用于首次连接 bind 回包
            public static string BuildBindAckJson(Guid clientId)
            {
                var sb = new StringBuilder(128);
                sb.Append('{');
                WritePropName(sb, "type"); WriteString(sb, WsType.bind.ToString()); sb.Append(',');
                WritePropName(sb, "clientId"); WriteString(sb, clientId.ToString("D")); sb.Append(',');
                WritePropName(sb, "targetId"); WriteString(sb, string.Empty); sb.Append(',');
                WritePropName(sb, "message"); WriteString(sb, "targetId");
                sb.Append('}');
                return sb.ToString();
            }

            public static string BuildStringArrayJson(IEnumerable<string> items)
            {
                var sb = new StringBuilder();
                sb.Append('[');
                bool first = true;
                foreach (var s in items)
                {
                    if (!first) sb.Append(',');
                    first = false;
                    WriteString(sb, s ?? string.Empty);
                }
                sb.Append(']');
                return sb.ToString();
            }

            // Extract top-level "type" (string), "clientId", "targetId", and raw "message" value (JSON) from an object
            public static bool TryGetTypeAndMessage(string json, out string type, out string messageRaw)
            {
                return TryParseEnvelope(json, out type, out _, out _, out messageRaw);
            }

            public static bool TryParseEnvelope(string json, out string type, out string clientId, out string targetId, out string messageRaw)
            {
                type = null; clientId = null; targetId = null; messageRaw = null;
                if (string.IsNullOrEmpty(json)) return false;

                int i = 0;
                SkipWs(json, ref i);
                if (i >= json.Length || json[i] != '{') return false;
                i++; // skip {

                while (true)
                {
                    SkipWs(json, ref i);
                    if (i >= json.Length) return false;
                    if (json[i] == '}') { i++; break; }

                    // key
                    if (json[i] != '"') return false;
                    if (!TryParseString(json, ref i, out var key)) return false;

                    SkipWs(json, ref i);
                    if (i >= json.Length || json[i] != ':') return false;
                    i++; // :

                    SkipWs(json, ref i);
                    if (i >= json.Length) return false;

                    // value
                    int valueStart = i;
                    if (!TryScanValue(json, ref i)) return false;
                    int valueEnd = i; // exclusive

                    if (key == "type")
                    {
                        // must be string
                        int t = valueStart;
                        SkipWs(json, ref t);
                        if (t < valueEnd && json[t] == '"')
                        {
                            if (TryParseString(json, ref t, out var tval)) type = tval;
                        }
                    }
                    else if (key == "clientId")
                    {
                        int t = valueStart;
                        SkipWs(json, ref t);
                        if (t < valueEnd && json[t] == '"')
                        {
                            if (TryParseString(json, ref t, out var cval)) clientId = cval;
                        }
                    }
                    else if (key == "targetId")
                    {
                        int t = valueStart;
                        SkipWs(json, ref t);
                        if (t < valueEnd && json[t] == '"')
                        {
                            if (TryParseString(json, ref t, out var tval)) targetId = tval;
                        }
                    }
                    else if (key == "message")
                    {
                        messageRaw = json.Substring(valueStart, valueEnd - valueStart);
                    }

                    SkipWs(json, ref i);
                    if (i >= json.Length) return false;
                    if (json[i] == ',') { i++; continue; }
                    if (json[i] == '}') { i++; break; }
                    return false;
                }

                return type != null; // message may be absent in heartbeats
            }

            // ---------- writers ----------
            private static void WritePropName(StringBuilder sb, string name)
            {
                WriteString(sb, name);
                sb.Append(':');
            }

            private static void WriteGuidPropIfAny(StringBuilder sb, string name, Guid? value)
            {
                if (!value.HasValue) return;
                WritePropName(sb, name);
                WriteString(sb, value.Value.ToString("D"));
                sb.Append(',');
            }

            private static void WriteString(StringBuilder sb, string s)
            {
                sb.Append('"');
                if (!string.IsNullOrEmpty(s))
                {
                    for (int i = 0; i < s.Length; i++)
                    {
                        char c = s[i];
                        switch (c)
                        {
                            case '"': sb.Append("\\\""); break;
                            case '\\': sb.Append("\\\\"); break;
                            case '\b': sb.Append("\\b"); break;
                            case '\f': sb.Append("\\f"); break;
                            case '\n': sb.Append("\\n"); break;
                            case '\r': sb.Append("\\r"); break;
                            case '\t': sb.Append("\\t"); break;
                            default:
                                if (c < 0x20 || (c >= 0xD800 && c <= 0xDFFF)) // control or surrogate
                                {
                                    sb.Append("\\u");
                                    sb.Append(((int)c).ToString("X4"));
                                }
                                else
                                {
                                    sb.Append(c);
                                }
                                break;
                        }
                    }
                }
                sb.Append('"');
            }

            // ---------- readers ----------
            private static void SkipWs(string s, ref int i)
            {
                while (i < s.Length)
                {
                    char c = s[i];
                    if (c == ' ' || c == '\t' || c == '\r' || c == '\n') { i++; continue; }
                    break;
                }
            }

            private static bool TryParseString(string s, ref int i, out string value)
            {
                value = null;
                if (i >= s.Length || s[i] != '"') return false;
                i++; // skip opening quote

                var sb = new StringBuilder();
                while (i < s.Length)
                {
                    char c = s[i++];
                    if (c == '"') { value = sb.ToString(); return true; }
                    if (c == '\\')
                    {
                        if (i >= s.Length) return false;
                        char e = s[i++];
                        switch (e)
                        {
                            case '"': sb.Append('"'); break;
                            case '\\': sb.Append('\\'); break;
                            case '/': sb.Append('/'); break;
                            case 'b': sb.Append('\b'); break;
                            case 'f': sb.Append('\f'); break;
                            case 'n': sb.Append('\n'); break;
                            case 'r': sb.Append('\r'); break;
                            case 't': sb.Append('\t'); break;
                            case 'u':
                                if (i + 4 > s.Length) return false;
                                if (!TryHex4(s, i, out var ch)) return false;
                                sb.Append(ch);
                                i += 4;
                                break;
                            default: return false;
                        }
                    }
                    else
                    {
                        sb.Append(c);
                    }
                }
                return false;
            }

            private static bool TryHex4(string s, int i, out char ch)
            {
                ch = '\0';
                if (i + 4 > s.Length) return false;
                int v = 0;
                for (int k = 0; k < 4; k++)
                {
                    char c = s[i + k];
                    int d;
                    if (c >= '0' && c <= '9') d = c - '0';
                    else if (c >= 'a' && c <= 'f') d = 10 + (c - 'a');
                    else if (c >= 'A' && c <= 'F') d = 10 + (c - 'A');
                    else return false;
                    v = (v << 4) | d;
                }
                ch = (char)v;
                return true;
            }

            // Scan a JSON value and advance i to the first char AFTER the value
            private static bool TryScanValue(string s, ref int i)
            {
                if (i >= s.Length) return false;
                char c = s[i];
                if (c == '"') { return TryScanString(s, ref i); }
                if (c == '{') { return TryScanBlock(s, ref i, '{', '}'); }
                if (c == '[') { return TryScanBlock(s, ref i, '[', ']'); }
                if (c == 't') return TryScanKeyword(s, ref i, "true");
                if (c == 'f') return TryScanKeyword(s, ref i, "false");
                if (c == 'n') return TryScanKeyword(s, ref i, "null");
                return TryScanNumber(s, ref i);
            }

            private static bool TryScanString(string s, ref int i)
            {
                if (i >= s.Length || s[i] != '"') return false;
                i++;
                while (i < s.Length)
                {
                    char c = s[i++];
                    if (c == '"') return true;
                    if (c == '\\')
                    {
                        if (i >= s.Length) return false;
                        char e = s[i++];
                        if (e == 'u')
                        {
                            if (i + 4 > s.Length) return false;
                            for (int k = 0; k < 4; k++)
                            {
                                char h = s[i + k];
                                bool ok = (h >= '0' && h <= '9') || (h >= 'a' && h <= 'f') || (h >= 'A' && h <= 'F');
                                if (!ok) return false;
                            }
                            i += 4;
                        }
                        // else: skip simple escape
                    }
                }
                return false;
            }

            private static bool TryScanBlock(string s, ref int i, char open, char close)
            {
                if (i >= s.Length || s[i] != open) return false;
                int depth = 0;
                while (i < s.Length)
                {
                    char c = s[i++];
                    if (c == '"')
                    {
                        i--; // step back to quote to reuse string scan
                        if (!TryScanString(s, ref i)) return false;
                        continue;
                    }
                    if (c == open) depth++;
                    else if (c == close)
                    {
                        depth--;
                        if (depth == 0) return true;
                    }
                }
                return false;
            }

            private static bool TryScanKeyword(string s, ref int i, string kw)
            {
                if (i + kw.Length > s.Length) return false;
                for (int k = 0; k < kw.Length; k++)
                    if (s[i + k] != kw[k]) return false;
                i += kw.Length;
                return true;
            }

            private static bool TryScanNumber(string s, ref int i)
            {
                int start = i;
                if (i < s.Length && (s[i] == '-' || s[i] == '+')) i++;
                bool hasDigits = false;
                while (i < s.Length && char.IsDigit(s[i])) { i++; hasDigits = true; }
                if (i < s.Length && s[i] == '.') { i++; while (i < s.Length && char.IsDigit(s[i])) { i++; hasDigits = true; } }
                if (i < s.Length && (s[i] == 'e' || s[i] == 'E'))
                {
                    i++;
                    if (i < s.Length && (s[i] == '+' || s[i] == '-')) i++;
                    bool any = false;
                    while (i < s.Length && char.IsDigit(s[i])) { i++; any = true; }
                    hasDigits |= any;
                }
                return hasDigits && i > start;
            }
        }
    }
}
