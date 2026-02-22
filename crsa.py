"""
CRS Normalize — прототип конструктора систем координат
Зависимости: pip install customtkinter pyproj
Запуск:      python crs_normalize.py
"""

from __future__ import annotations

import json
from typing import Optional

import customtkinter as ctk
from pyproj import CRS
from pyproj.exceptions import CRSError

# ── Тема ──────────────────────────────────────────────────────────────────────
ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("blue")

# ── Справочники ───────────────────────────────────────────────────────────────
ELLIPSOIDS = ["WGS84", "GRS80", "Красовский", "ПЗ-90", "CGCS2000", "Другой"]

DATUMS_GCS = [
    "WGS84", "ПЗ-90.02", "ПЗ-90.11",
    "СК-42", "СК-95", "ГСК-2011", "CGCS2000", "Другой",
]
DATUMS_GK = ["СК-42", "СК-95", "ГСК-2011", "ПЗ-90.02", "ПЗ-90.11", "WGS84", "Другой"]
DATUMS_MSK = ["СК-42", "СК-95", "ГСК-2011", "Другой"]
DATUMS_UTM = ["WGS84", "ГСК-2011", "Другой"]
DATUMS_MERC = ["WGS84", "ПЗ-90.11", "Другой"]
DATUMS_OTHER = ["WGS84", "СК-42", "СК-95", "ГСК-2011", "ПЗ-90.02", "ПЗ-90.11", "CGCS2000", "Другой"]

PROJECTIONS = ["Гаусс-Крюгер", "МСК", "UTM", "Меркатор", "Другая"]

# Маппинг датумов на EPSG для автоматической сборки CRS
DATUM_TO_EPSG: dict[str, int] = {
    "WGS84":     4326,
    "ПЗ-90.02":  4922,
    "ПЗ-90.11":  7679,
    "СК-42":     4284,
    "СК-95":     4815,
    "ГСК-2011":  7683,
    "CGCS2000":  4490,
}

ELLIPSOID_PARAMS: dict[str, dict] = {
    "WGS84":      {"semi_major_axis": 6378137.0,   "inverse_flattening": 298.257223563},
    "GRS80":      {"semi_major_axis": 6378137.0,   "inverse_flattening": 298.257222101},
    "Красовский": {"semi_major_axis": 6378245.0,   "inverse_flattening": 298.3},
    "ПЗ-90":      {"semi_major_axis": 6378136.0,   "inverse_flattening": 298.257839303},
    "CGCS2000":   {"semi_major_axis": 6378137.0,   "inverse_flattening": 298.257222101},
}

# ── Вспомогательные виджеты ───────────────────────────────────────────────────

def labeled_entry(parent, label: str, row: int, default: str = "") -> ctk.CTkEntry:
    ctk.CTkLabel(parent, text=label).grid(row=row, column=0, sticky="w", padx=10, pady=4)
    e = ctk.CTkEntry(parent, width=220)
    e.insert(0, default)
    e.grid(row=row, column=1, padx=10, pady=4)
    return e

def labeled_combo(parent, label: str, values: list[str], row: int) -> ctk.CTkComboBox:
    ctk.CTkLabel(parent, text=label).grid(row=row, column=0, sticky="w", padx=10, pady=4)
    c = ctk.CTkComboBox(parent, values=values, width=220, state="readonly")
    c.set(values[0])
    c.grid(row=row, column=1, padx=10, pady=4)
    return c

# ── Главное окно / роутер ─────────────────────────────────────────────────────

class App(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("CRS Normalize")
        self.geometry("780x620")
        self.resizable(False, False)

        self._state: dict = {}          # накопленные параметры
        self._frame: Optional[ctk.CTkFrame] = None
        self._show(ModeScreen)

    # ── навигация ─────────────────────────────────────────────────────────────
    def _show(self, screen_cls, **kwargs):
        if self._frame:
            self._frame.destroy()
        self._frame = screen_cls(self, self._state, self._show, **kwargs)
        self._frame.pack(fill="both", expand=True, padx=20, pady=20)

    def go(self, screen_cls, **kwargs):
        self._show(screen_cls, **kwargs)

# ── Базовый экран ─────────────────────────────────────────────────────────────

class BaseScreen(ctk.CTkFrame):
    def __init__(self, master: App, state: dict, navigate, **kwargs):
        super().__init__(master)
        self.app = master
        self.state = state
        self.navigate = navigate
        self._error_label: Optional[ctk.CTkLabel] = None
        self.build()

    def build(self):
        pass

    def title_label(self, text: str):
        ctk.CTkLabel(self, text=text, font=ctk.CTkFont(size=18, weight="bold")).pack(pady=(10, 18))

    def error(self, msg: str):
        if self._error_label:
            self._error_label.configure(text=msg)
        else:
            self._error_label = ctk.CTkLabel(self, text=msg, text_color="tomato")
            self._error_label.pack(pady=4)

    def clear_error(self):
        if self._error_label:
            self._error_label.configure(text="")

    def back_btn(self, target):
        ctk.CTkButton(self, text="← Назад", width=100,
                      fg_color="gray30", hover_color="gray40",
                      command=lambda: self.navigate(target)).pack(pady=(12, 0))

# ══════════════════════════════════════════════════════════════════════════════
# 1. Выбор режима
# ══════════════════════════════════════════════════════════════════════════════

class ModeScreen(BaseScreen):
    def build(self):
        self.title_label("CRS Normalize — Выберите режим")
        for text, screen in [
            ("🔢  Знаю EPSG код",          EpsgScreen),
            ("📋  Вставить WKT2 / PROJJSON", RawScreen),
            ("🧙  Собрать вручную (Wizard)", WizardTypeScreen),
        ]:
            ctk.CTkButton(self, text=text, width=320, height=48,
                          font=ctk.CTkFont(size=14),
                          command=lambda s=screen: self.navigate(s)
                          ).pack(pady=8)

# ══════════════════════════════════════════════════════════════════════════════
# 2. EPSG
# ══════════════════════════════════════════════════════════════════════════════

class EpsgScreen(BaseScreen):
    def build(self):
        self.title_label("Ввод EPSG кода")
        self.entry = ctk.CTkEntry(self, placeholder_text="например: 32637", width=260, height=40)
        self.entry.pack(pady=8)
        ctk.CTkButton(self, text="Определить →", width=200,
                      command=self._resolve).pack(pady=8)
        self.back_btn(ModeScreen)

    def _resolve(self):
        code = self.entry.get().strip()
        try:
            epsg = int(code)
            crs = CRS.from_epsg(epsg)
            self.state["crs_source"] = "epsg"
            self.state["crs_obj"] = crs
            self.navigate(PreviewScreen)
        except (ValueError, CRSError):
            self.error(f"EPSG:{code} не найден в базе PROJ. Проверьте код.")

# ══════════════════════════════════════════════════════════════════════════════
# 3. RAW WKT2 / PROJJSON
# ══════════════════════════════════════════════════════════════════════════════

class RawScreen(BaseScreen):
    def build(self):
        self.title_label("Вставить WKT2 или PROJJSON")
        self.text = ctk.CTkTextbox(self, width=680, height=300, font=ctk.CTkFont(family="Courier", size=12))
        self.text.pack(pady=8)
        ctk.CTkButton(self, text="Разобрать →", width=200,
                      command=self._parse).pack(pady=8)
        self.back_btn(ModeScreen)

    def _parse(self):
        raw = self.text.get("1.0", "end").strip()
        try:
            crs = CRS.from_user_input(raw)
            self.state["crs_source"] = "raw"
            self.state["crs_obj"] = crs
            self.navigate(PreviewScreen)
        except CRSError as e:
            self.error(f"Ошибка парсинга: {e}")

# ══════════════════════════════════════════════════════════════════════════════
# 4. Wizard — тип СК
# ══════════════════════════════════════════════════════════════════════════════

class WizardTypeScreen(BaseScreen):
    def build(self):
        self.title_label("Wizard — Тип системы координат")
        for text, tag in [("🌐  Географическая (GCS)", "GCS"), ("🗺  Проецированная (PCS)", "PCS")]:
            ctk.CTkButton(self, text=text, width=280, height=44,
                          font=ctk.CTkFont(size=13),
                          command=lambda t=tag: self._pick(t)).pack(pady=8)
        self.back_btn(ModeScreen)

    def _pick(self, crs_type: str):
        self.state["wiz_type"] = crs_type
        if crs_type == "GCS":
            self.navigate(WizardGCSScreen)
        else:
            self.navigate(WizardEllipsoidScreen)

# ══════════════════════════════════════════════════════════════════════════════
# 4a. Wizard GCS
# ══════════════════════════════════════════════════════════════════════════════

class WizardGCSScreen(BaseScreen):
    def build(self):
        self.title_label("Wizard — Географическая СК")
        f = ctk.CTkFrame(self, fg_color="transparent")
        f.pack(pady=8)
        self.ellipsoid = labeled_combo(f, "Эллипсоид:", ELLIPSOIDS, 0)
        self.datum = labeled_combo(f, "Датум:", DATUMS_GCS, 1)

        # поле «Другой» для эллипсоида
        ctk.CTkLabel(f, text="(если Другой) a / rf:").grid(row=2, column=0, sticky="w", padx=10, pady=4)
        self.ell_custom = ctk.CTkEntry(f, placeholder_text="6378137.0 / 298.257", width=220)
        self.ell_custom.grid(row=2, column=1, padx=10, pady=4)

        ctk.CTkButton(self, text="Далее →", width=200, command=self._next).pack(pady=12)
        self.back_btn(WizardTypeScreen)

    def _next(self):
        self.state["wiz_ellipsoid"] = self.ellipsoid.get()
        self.state["wiz_datum"] = self.datum.get()
        self.state["wiz_ell_custom"] = self.ell_custom.get()
        self.navigate(TowgsScreen)

# ══════════════════════════════════════════════════════════════════════════════
# 4b. Wizard PCS — эллипсоид
# ══════════════════════════════════════════════════════════════════════════════

class WizardEllipsoidScreen(BaseScreen):
    def build(self):
        self.title_label("Wizard — Эллипсоид")
        f = ctk.CTkFrame(self, fg_color="transparent")
        f.pack(pady=8)
        self.ellipsoid = labeled_combo(f, "Эллипсоид:", ELLIPSOIDS, 0)
        ctk.CTkLabel(f, text="(если Другой) a / rf:").grid(row=1, column=0, sticky="w", padx=10, pady=4)
        self.ell_custom = ctk.CTkEntry(f, placeholder_text="6378137.0 / 298.257", width=220)
        self.ell_custom.grid(row=1, column=1, padx=10, pady=4)

        ctk.CTkButton(self, text="Далее →", width=200, command=self._next).pack(pady=12)
        self.back_btn(WizardTypeScreen)

    def _next(self):
        self.state["wiz_ellipsoid"] = self.ellipsoid.get()
        self.state["wiz_ell_custom"] = self.ell_custom.get()
        self.navigate(WizardProjScreen)

# ══════════════════════════════════════════════════════════════════════════════
# 4c. Wizard PCS — проекция
# ══════════════════════════════════════════════════════════════════════════════

class WizardProjScreen(BaseScreen):
    def build(self):
        self.title_label("Wizard — Тип проекции")
        for proj in PROJECTIONS:
            ctk.CTkButton(self, text=proj, width=260, height=40,
                          command=lambda p=proj: self._pick(p)).pack(pady=6)
        self.back_btn(WizardEllipsoidScreen)

    def _pick(self, proj: str):
        self.state["wiz_proj"] = proj
        screens = {
            "Гаусс-Крюгер": WizardGKScreen,
            "МСК":           WizardMSKScreen,
            "UTM":           WizardUTMScreen,
            "Меркатор":      WizardMercScreen,
            "Другая":        WizardOtherProjScreen,
        }
        self.navigate(screens[proj])

# ══════════════════════════════════════════════════════════════════════════════
# 4d. Параметры проекций
# ══════════════════════════════════════════════════════════════════════════════

class WizardGKScreen(BaseScreen):
    def build(self):
        self.title_label("Гаусс-Крюгер — параметры")
        f = ctk.CTkFrame(self, fg_color="transparent")
        f.pack(pady=8)
        self.meridian  = labeled_entry(f, "Осевой меридиан (°):", 0, "39")
        self.false_e   = labeled_entry(f, "Ложный восток (м):",   1, "500000")
        self.false_n   = labeled_entry(f, "Ложный север (м):",    2, "0")
        self.scale     = labeled_entry(f, "Масштаб:",             3, "1.0")
        self.datum     = labeled_combo(f, "Датум:", DATUMS_GK,    4)
        ctk.CTkButton(self, text="Далее →", width=200, command=self._next).pack(pady=12)
        self.back_btn(WizardProjScreen)

    def _next(self):
        self.state.update({
            "wiz_meridian": self.meridian.get(),
            "wiz_false_e":  self.false_e.get(),
            "wiz_false_n":  self.false_n.get(),
            "wiz_scale":    self.scale.get(),
            "wiz_datum":    self.datum.get(),
        })
        self.navigate(TowgsScreen)

class WizardMSKScreen(BaseScreen):
    def build(self):
        self.title_label("МСК — параметры")
        f = ctk.CTkFrame(self, fg_color="transparent")
        f.pack(pady=8)
        self.zone      = labeled_entry(f, "Номер зоны / субъекта:", 0, "")
        self.meridian  = labeled_entry(f, "Осевой меридиан (°):",   1, "")
        self.false_e   = labeled_entry(f, "Ложный восток (м):",     2, "")
        self.false_n   = labeled_entry(f, "Ложный север (м):",      3, "")
        self.scale     = labeled_entry(f, "Масштаб:",               4, "1.0")
        self.datum     = labeled_combo(f, "Датум:", DATUMS_MSK,     5)
        ctk.CTkButton(self, text="Далее →", width=200, command=self._next).pack(pady=12)
        self.back_btn(WizardProjScreen)

    def _next(self):
        self.state.update({
            "wiz_msk_zone": self.zone.get(),
            "wiz_meridian": self.meridian.get(),
            "wiz_false_e":  self.false_e.get(),
            "wiz_false_n":  self.false_n.get(),
            "wiz_scale":    self.scale.get(),
            "wiz_datum":    self.datum.get(),
        })
        self.navigate(TowgsScreen)

class WizardUTMScreen(BaseScreen):
    def build(self):
        self.title_label("UTM — параметры")
        f = ctk.CTkFrame(self, fg_color="transparent")
        f.pack(pady=8)
        self.zone  = labeled_entry(f, "Зона (1-60):", 0, "37")
        self.hemi  = labeled_combo(f, "Полушарие:",   ["N", "S"], 1)
        self.datum = labeled_combo(f, "Датум:", DATUMS_UTM, 2)
        ctk.CTkButton(self, text="Далее →", width=200, command=self._next).pack(pady=12)
        self.back_btn(WizardProjScreen)

    def _next(self):
        self.state.update({
            "wiz_utm_zone": self.zone.get(),
            "wiz_utm_hemi": self.hemi.get(),
            "wiz_datum":    self.datum.get(),
        })
        self.navigate(TowgsScreen)

class WizardMercScreen(BaseScreen):
    def build(self):
        self.title_label("Меркатор — параметры")
        f = ctk.CTkFrame(self, fg_color="transparent")
        f.pack(pady=8)
        self.parallel = labeled_entry(f, "Стандартная параллель (°):", 0, "0")
        self.false_e  = labeled_entry(f, "Ложный восток (м):",         1, "0")
        self.false_n  = labeled_entry(f, "Ложный север (м):",          2, "0")
        self.datum    = labeled_combo(f, "Датум:", DATUMS_MERC,         3)
        ctk.CTkButton(self, text="Далее →", width=200, command=self._next).pack(pady=12)
        self.back_btn(WizardProjScreen)

    def _next(self):
        self.state.update({
            "wiz_merc_parallel": self.parallel.get(),
            "wiz_false_e":       self.false_e.get(),
            "wiz_false_n":       self.false_n.get(),
            "wiz_datum":         self.datum.get(),
        })
        self.navigate(TowgsScreen)

class WizardOtherProjScreen(BaseScreen):
    def build(self):
        self.title_label("Другая проекция — параметры")
        f = ctk.CTkFrame(self, fg_color="transparent")
        f.pack(pady=8)
        self.name  = labeled_entry(f, "Название проекции (PROJ):", 0, "")
        self.params = ctk.CTkTextbox(self, width=640, height=120,
                                      font=ctk.CTkFont(family="Courier", size=11))
        self.params.pack(pady=4)
        ctk.CTkLabel(self, text="Параметры в формате  ключ=значение  через пробел",
                     text_color="gray60").pack()
        self.datum = labeled_combo(f, "Датум:", DATUMS_OTHER, 1)
        ctk.CTkButton(self, text="Далее →", width=200, command=self._next).pack(pady=12)
        self.back_btn(WizardProjScreen)

    def _next(self):
        self.state.update({
            "wiz_other_proj_name":   self.name.get(),
            "wiz_other_proj_params": self.params.get("1.0", "end").strip(),
            "wiz_datum":             self.datum.get(),
        })
        self.navigate(TowgsScreen)

# ══════════════════════════════════════════════════════════════════════════════
# 5. towgs84
# ══════════════════════════════════════════════════════════════════════════════

class TowgsScreen(BaseScreen):
    def build(self):
        self.title_label("Параметры трансформации к WGS84")
        ctk.CTkLabel(self, text="Нужны кастомные параметры Бурсы–Вольфа?").pack(pady=4)

        self.need = ctk.StringVar(value="no")
        row = ctk.CTkFrame(self, fg_color="transparent")
        row.pack(pady=6)
        ctk.CTkRadioButton(row, text="Нет / уже в датуме", variable=self.need,
                           value="no",  command=self._toggle).pack(side="left", padx=16)
        ctk.CTkRadioButton(row, text="Ввести вручную",     variable=self.need,
                           value="yes", command=self._toggle).pack(side="left", padx=16)

        self.form = ctk.CTkFrame(self, fg_color="transparent")
        self.form.pack(pady=8)
        labels = ["dX (м)", "dY (м)", "dZ (м)", "rx (″)", "ry (″)", "rz (″)", "масштаб (ppm)"]
        self.entries: list[ctk.CTkEntry] = []
        for i, lbl in enumerate(labels):
            ctk.CTkLabel(self.form, text=lbl, width=100).grid(row=0, column=i, padx=4)
            e = ctk.CTkEntry(self.form, width=80)
            e.insert(0, "0")
            e.grid(row=1, column=i, padx=4, pady=4)
            self.entries.append(e)

        self._toggle()
        ctk.CTkButton(self, text="Далее →", width=200, command=self._next).pack(pady=12)
        # кнопка назад ведёт в зависимости от пути
        prev = self._prev_screen()
        self.back_btn(prev)

    def _prev_screen(self):
        proj = self.state.get("wiz_proj")
        if self.state.get("wiz_type") == "GCS":
            return WizardGCSScreen
        mapping = {
            "Гаусс-Крюгер": WizardGKScreen,
            "МСК":           WizardMSKScreen,
            "UTM":           WizardUTMScreen,
            "Меркатор":      WizardMercScreen,
            "Другая":        WizardOtherProjScreen,
        }
        return mapping.get(proj, WizardProjScreen)

    def _toggle(self):
        state = "normal" if self.need.get() == "yes" else "disabled"
        for e in self.entries:
            e.configure(state=state)

    def _next(self):
        if self.need.get() == "yes":
            vals = []
            for e in self.entries:
                try:
                    vals.append(float(e.get()))
                except ValueError:
                    self.error("Все 7 параметров должны быть числами")
                    return
            self.state["wiz_towgs84"] = vals
        else:
            self.state["wiz_towgs84"] = None
        self._build_crs()

    def _build_crs(self):
        """Собираем CRS из накопленного state и кладём в state['crs_obj']."""
        try:
            crs = _build_crs_from_state(self.state)
            self.state["crs_obj"] = crs
            self.state["crs_source"] = "wizard"
            self.navigate(PreviewScreen)
        except Exception as e:
            self.error(f"Не удалось собрать CRS: {e}")

# ══════════════════════════════════════════════════════════════════════════════
# 6. Финальный предпросмотр
# ══════════════════════════════════════════════════════════════════════════════

class PreviewScreen(BaseScreen):
    def build(self):
        self.title_label("Предпросмотр — итоговый PROJJSON")
        crs: CRS = self.state.get("crs_obj")

        if crs is None:
            self.error("CRS не определён — вернитесь назад")
            self.back_btn(ModeScreen)
            return

        projjson = json.loads(crs.to_json())

        # краткая сводка
        info_frame = ctk.CTkFrame(self)
        info_frame.pack(fill="x", padx=4, pady=4)
        pairs = [
            ("Название",  crs.name),
            ("Тип",       crs.type_name),
            ("Единицы",   str(crs.axis_info[0].unit_name) if crs.axis_info else "—"),
        ]
        for i, (k, v) in enumerate(pairs):
            ctk.CTkLabel(info_frame, text=f"{k}:", font=ctk.CTkFont(weight="bold"),
                         width=90, anchor="e").grid(row=i, column=0, padx=(10, 4), pady=2)
            ctk.CTkLabel(info_frame, text=v, anchor="w").grid(row=i, column=1, sticky="w")

        # PROJJSON текст
        self.textbox = ctk.CTkTextbox(self, width=700, height=280,
                                       font=ctk.CTkFont(family="Courier", size=11))
        self.textbox.pack(pady=8)
        self.textbox.insert("1.0", json.dumps(projjson, ensure_ascii=False, indent=2))
        self.textbox.configure(state="disabled")

        btn_row = ctk.CTkFrame(self, fg_color="transparent")
        btn_row.pack(pady=8)
        ctk.CTkButton(btn_row, text="✏  Исправить", width=160,
                      fg_color="gray30", hover_color="gray40",
                      command=lambda: self.navigate(ModeScreen)).pack(side="left", padx=10)
        ctk.CTkButton(btn_row, text="✔  Подтвердить", width=160,
                      command=self._confirm).pack(side="left", padx=10)

    def _confirm(self):
        self.navigate(OutputScreen)

# ══════════════════════════════════════════════════════════════════════════════
# 7. Результат
# ══════════════════════════════════════════════════════════════════════════════

class OutputScreen(BaseScreen):
    def build(self):
        self.title_label("✅  PROJJSON готов")
        crs: CRS = self.state["crs_obj"]
        projjson_str = crs.to_json()

        ctk.CTkLabel(self, text="PROJJSON передан бэкенду (в реальном приложении — POST/S3 upload).",
                     text_color="gray70").pack(pady=4)

        box = ctk.CTkTextbox(self, width=700, height=320,
                              font=ctk.CTkFont(family="Courier", size=11))
        box.pack(pady=8)
        box.insert("1.0", projjson_str)
        box.configure(state="disabled")

        btn_row = ctk.CTkFrame(self, fg_color="transparent")
        btn_row.pack(pady=8)
        ctk.CTkButton(btn_row, text="📋  Скопировать", width=160,
                      command=lambda: self._copy(projjson_str)).pack(side="left", padx=10)
        ctk.CTkButton(btn_row, text="💾  Сохранить .json", width=160,
                      command=lambda: self._save(projjson_str)).pack(side="left", padx=10)
        ctk.CTkButton(btn_row, text="🔄  Новый", width=140,
                      fg_color="gray30", hover_color="gray40",
                      command=self._reset).pack(side="left", padx=10)

    def _copy(self, text: str):
        self.clipboard_clear()
        self.clipboard_append(text)

    def _save(self, text: str):
        from tkinter import filedialog
        path = filedialog.asksaveasfilename(defaultextension=".json",
                                            filetypes=[("JSON", "*.json"), ("All", "*.*")])
        if path:
            with open(path, "w", encoding="utf-8") as f:
                f.write(text)

    def _reset(self):
        self.state.clear()
        self.navigate(ModeScreen)

# ══════════════════════════════════════════════════════════════════════════════
# CRS builder — сборка CRS из wizard state
# ══════════════════════════════════════════════════════════════════════════════

def _build_crs_from_state(state: dict) -> CRS:
    """
    Собирает pyproj.CRS из накопленных параметров wizard.
    Использует PROJ строку как промежуточный формат — надёжно и прозрачно.
    """
    crs_type  = state.get("wiz_type", "PCS")
    ellipsoid = state.get("wiz_ellipsoid", "WGS84")
    datum     = state.get("wiz_datum", "WGS84")
    towgs84   = state.get("wiz_towgs84")          # list[float] | None

    # --- эллипсоид ---
    ell_params = ELLIPSOID_PARAMS.get(ellipsoid)
    if ell_params:
        ell_str = f"+a={ell_params['semi_major_axis']} +rf={ell_params['inverse_flattening']}"
    else:
        # Другой — пробуем распарсить "a / rf" из поля
        custom = state.get("wiz_ell_custom", "")
        parts = [p.strip() for p in custom.split("/")]
        if len(parts) == 2:
            ell_str = f"+a={parts[0]} +rf={parts[1]}"
        else:
            raise ValueError("Для эллипсоида 'Другой' укажите a / rf через /")

    # --- towgs84 суффикс ---
    tgs = ""
    if towgs84:
        tgs = "+towgs84=" + ",".join(str(v) for v in towgs84)

    if crs_type == "GCS":
        proj_str = f"+proj=longlat {ell_str} {tgs} +no_defs"
        return CRS.from_proj4(proj_str)

    # --- PCS ---
    proj = state.get("wiz_proj")

    if proj == "Гаусс-Крюгер":
        lon0 = state.get("wiz_meridian", "39")
        fe   = state.get("wiz_false_e", "500000")
        fn   = state.get("wiz_false_n", "0")
        k    = state.get("wiz_scale", "1.0")
        proj_str = (f"+proj=tmerc +lat_0=0 +lon_0={lon0} "
                    f"+k={k} +x_0={fe} +y_0={fn} "
                    f"{ell_str} {tgs} +units=m +no_defs")

    elif proj == "МСК":
        lon0 = state.get("wiz_meridian", "39")
        fe   = state.get("wiz_false_e", "0")
        fn   = state.get("wiz_false_n", "0")
        k    = state.get("wiz_scale", "1.0")
        proj_str = (f"+proj=tmerc +lat_0=0 +lon_0={lon0} "
                    f"+k={k} +x_0={fe} +y_0={fn} "
                    f"{ell_str} {tgs} +units=m +no_defs")

    elif proj == "UTM":
        zone = state.get("wiz_utm_zone", "37")
        hemi = state.get("wiz_utm_hemi", "N")
        south = "+south" if hemi == "S" else ""
        proj_str = (f"+proj=utm +zone={zone} {south} "
                    f"{ell_str} {tgs} +units=m +no_defs")

    elif proj == "Меркатор":
        lat_ts = state.get("wiz_merc_parallel", "0")
        fe     = state.get("wiz_false_e", "0")
        fn     = state.get("wiz_false_n", "0")
        proj_str = (f"+proj=merc +lat_ts={lat_ts} "
                    f"+x_0={fe} +y_0={fn} "
                    f"{ell_str} {tgs} +units=m +no_defs")

    elif proj == "Другая":
        name   = state.get("wiz_other_proj_name", "")
        params = state.get("wiz_other_proj_params", "")
        proj_str = f"+proj={name} {params} {ell_str} {tgs} +units=m +no_defs"

    else:
        raise ValueError(f"Неизвестная проекция: {proj}")

    return CRS.from_proj4(proj_str)


# ── Запуск ────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    app = App()
    app.mainloop()