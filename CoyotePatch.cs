using DB;
using HarmonyLib;
using MAI2.Util;
using Manager;
using MelonLoader;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DG_CoyoteDX
{
    [HarmonyPatch]
    static class CoyotePatch
    {
        [HarmonyPatch(typeof(GameScoreList), nameof(GameScoreList.SetResult))]
        [HarmonyPostfix]
        static void SetResult(GameScoreList __instance, int index, NoteScore.EScoreType kind, NoteJudge.ETiming timing)
        {
            try
            {
                if (__instance == null) return;
                if (!__instance.IsHuman()) return;
                if (__instance.PlayerIndex != 0) return; //for 1P only;
                if (__instance.IsTrackSkip || __instance.IsLifeTrackSkip) return;
                if (GameManager.IsAdvDemo) return;
                if (GamePlayManager.Instance.IsQuickRetry()) return;

                // Melon<Core>.Logger.Msg($"index={index} kind={kind} timing={timing}");
                NoteJudge.JudgeBox box = NoteJudge.ConvertJudge(timing);
                switch (box)
                {
                    case NoteJudge.JudgeBox.Critical:
                    case NoteJudge.JudgeBox.Perfect:
                        break;
                    case NoteJudge.JudgeBox.Great:
                        Melon<Core>.Instance.Punish(25, 25);
                        break;
                    case NoteJudge.JudgeBox.Good:
                        Melon<Core>.Instance.Punish(50, 25);
                        break;
                    case NoteJudge.JudgeBox.Miss:
                        Melon<Core>.Instance.Punish(100, 25);
                        break;
                }
            }
            catch (Exception e)
            {
                Melon<Core>.Logger.Error(e);
            }
        }
    }
}
