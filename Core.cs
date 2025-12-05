using HarmonyLib;
using Manager;
using MelonLoader;
using QRCoder;
using System.Windows;
using UnityEngine;
using static DG_CoyoteDX.DGSocketServer;
using Rect = UnityEngine.Rect;

[assembly: MelonInfo(typeof(DG_CoyoteDX.Core), "DG-CoyoteDX", "1.0.0", "xiaodai", null)]
[assembly: MelonGame("sega-interactive", "Sinmai")]

namespace DG_CoyoteDX
{
    public class Core : MelonMod
    {

        private DGSocketServer wsServer;
        private DGSocketServer.GeneratedLink wsClientInfo;
        
        public override void OnInitializeMelon()
        {
            // HarmonyLib.Harmony.CreateAndPatchAll(typeof(CoyotePatch));
            wsServer = new(new DGSocketServer.Options());
            wsServer.DebugSink = LoggerInstance.Msg;
            wsServer.OnBind += (lid, cid) =>
            {
                LoggerInstance.Msg($"Bind OK: client={cid} <-> target={lid}");
                isConnected = true;
            };
            wsServer.OnAppMessage += msg => LoggerInstance.Msg($"APP->SERVER: {msg}");
            wsServer.Start();
            wsClientInfo = wsServer.CreateLink();
            MelonEvents.OnGUI.Subscribe(DrawWindow, 100);

            LoggerInstance.Msg("Initialized.");
        }

        public void Punish(int strength, int duration)
        {
            punishmentQueue.Add(new() { strength = strength, duration = duration });
        }

        private bool isConnected;
        private Rect windowRect = new(0, 0, 300, 600);
        private Style Style = new Style();

        private void DrawWindow()
        {
            windowRect = GUI.Window(0, windowRect, WindowFunction, "CoyoteDX");
        }

        private string qrText;
        private Texture2D qrTexture;

        private void SetQRTexture(string text)
        {
            if (qrText == text)
            {
                return;
            }
            using (QRCodeGenerator gen = new QRCodeGenerator())
            using (QRCodeData data = gen.CreateQrCode(text, QRCodeGenerator.ECCLevel.Q))
            using (PngByteQRCode qrCode = new PngByteQRCode(data))
            {
                byte[] qrCodeAsBitmapByteArr = qrCode.GetGraphic(10, true);
                Texture2D texture = new Texture2D(1, 1);
                texture.LoadImage(qrCodeAsBitmapByteArr);
                qrTexture = texture;
                qrText = text;
            }
        }

        private void WindowFunction(int winId)
        {
            GUI.DragWindow(new Rect(0, 0, 10000, 20));
            GUILayout.Label($"Connected: {isConnected}");

            SetQRTexture(wsClientInfo.QrUrl);
            GUILayout.Box(qrTexture, GUILayout.Height(300));
            if (GUILayout.Button("Test Pulse"))
            {
                wsServer.SendPulsesAsync(wsClientInfo.LinkId, DGSocketServer.Channel.A, new DGSocketServer.PulseBuilder().AddConstant(5, 20));
            }
        }

        // for all judgements in one frame, send one pulse only
        struct PunishmentArgs
        {
            public int strength, duration;
        };

        private List<PunishmentArgs> punishmentQueue = new();

        public override void OnUpdate()
        {
            int punishStrength = punishmentQueue.Max(obj => obj.strength);
            var args = punishmentQueue.First(x => x.strength == punishStrength);
            wsServer.SendPulsesAsync(wsClientInfo.LinkId, DGSocketServer.Channel.A, new DGSocketServer.PulseBuilder().AddRamp(args.strength, 0, args.duration));
            punishmentQueue.Clear();
        }

    }
}
