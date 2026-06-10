import { FormEvent, useEffect, useState } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import { useAuth } from "@/context/AuthContext";
import type { IdentifierType, LoginRequest } from "@/types/auth";
import { authService } from "@/services/authService";
import styles from "./LoginPage.module.css";

type LocationState = {
  from?: string;
};

type LoginMode = "code" | "password";
type AccountType = "phone" | "email";

const LoginPage = () => {
  const navigate = useNavigate();
  const location = useLocation();
  const { login, isLoading, user } = useAuth();
  const [mode, setMode] = useState<LoginMode>("code");
  const [accountType, setAccountType] = useState<AccountType>("phone");
  const [identifier, setIdentifier] = useState("");
  const [code, setCode] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [sendingCode, setSendingCode] = useState(false);
  const [countdown, setCountdown] = useState(0);

  const from = (location.state as LocationState | undefined)?.from ?? "/";
  const identifierType: IdentifierType = accountType === "phone" ? "PHONE" : "EMAIL";

  useEffect(() => {
    if (!isLoading && user) {
      navigate(from, { replace: true });
    }
  }, [isLoading, user, navigate, from]);

  useEffect(() => {
    if (countdown <= 0) return;
    const timer = window.setTimeout(() => setCountdown(prev => prev - 1), 1000);
    return () => window.clearTimeout(timer);
  }, [countdown]);

  const handleSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setError(null);
    setSubmitting(true);

    try {
      const payload: LoginRequest = mode === "code"
        ? { identifierType, identifier, code }
        : { identifierType, identifier, password };
      await login(payload);
      navigate(from, { replace: true });
    } catch (err) {
      const message = err instanceof Error ? err.message : "登录失败，请稍后重试";
      setError(message);
    } finally {
      setSubmitting(false);
    }
  };

  const handleSendCode = async () => {
    if (!identifier) {
      setError(accountType === "phone" ? "请先填写手机号" : "请先填写邮箱");
      return;
    }
    setError(null);
    setSendingCode(true);
    try {
      const response = await authService.sendCode({
        scene: "LOGIN",
        identifierType,
        identifier
      });
      setCountdown(Math.max(1, response.expireSeconds ?? 300));
    } catch (err) {
      const info = err instanceof Error ? err.message : "验证码发送失败";
      setError(info);
    } finally {
      setSendingCode(false);
    }
  };

  const handleAccountTypeChange = (type: AccountType) => {
    setAccountType(type);
    setIdentifier("");
    setCode("");
    setPassword("");
    setError(null);
  };

  const isDisabled = submitting || !identifier || (mode === "code" ? !code : !password);

  return (
    <div className={styles.page}>
      <div className={styles.card}>
        <div className={styles.titleBlock}>
          <h1 className={styles.title}>欢迎回来</h1>
          <p className={styles.subtitle}>登录知光，与知识发光</p>
        </div>

        {/* 账号类型切换 */}
        <div className={styles.tabRow}>
          <button
            type="button"
            className={`${styles.tab} ${accountType === "phone" ? styles.tabActive : ""}`}
            onClick={() => handleAccountTypeChange("phone")}
          >
            手机号
          </button>
          <button
            type="button"
            className={`${styles.tab} ${accountType === "email" ? styles.tabActive : ""}`}
            onClick={() => handleAccountTypeChange("email")}
          >
            邮箱
          </button>
        </div>

        {/* 登录方式切换 */}
        <div className={styles.tabRow}>
          <button
            type="button"
            className={`${styles.tab} ${mode === "code" ? styles.tabActive : ""}`}
            onClick={() => { setMode("code"); setError(null); }}
          >
            验证码登录
          </button>
          <button
            type="button"
            className={`${styles.tab} ${mode === "password" ? styles.tabActive : ""}`}
            onClick={() => { setMode("password"); setError(null); }}
          >
            密码登录
          </button>
        </div>

        <form className={styles.form} onSubmit={handleSubmit}>
          <div className={styles.field}>
            <label className={styles.label} htmlFor="identifier">
              {accountType === "phone" ? "手机号" : "邮箱"}
            </label>
            <input
              id="identifier"
              className={styles.input}
              value={identifier}
              onChange={event => setIdentifier(event.target.value)}
              placeholder={accountType === "phone" ? "请输入手机号" : "请输入邮箱地址"}
              type={accountType === "phone" ? "tel" : "email"}
              autoComplete={accountType === "phone" ? "tel" : "email"}
            />
          </div>

          {mode === "code" ? (
            <div className={styles.field}>
              <label className={styles.label} htmlFor="code">
                验证码
              </label>
              <div className={styles.codeRow}>
                <input
                  id="code"
                  className={styles.input}
                  value={code}
                  onChange={event => setCode(event.target.value)}
                  placeholder="请输入验证码"
                  autoComplete="one-time-code"
                />
                <button
                  type="button"
                  className={styles.codeButton}
                  disabled={sendingCode || countdown > 0}
                  onClick={handleSendCode}
                >
                  {countdown > 0 ? `${countdown}s` : "获取验证码"}
                </button>
              </div>
              <span className={styles.tips}>
                {accountType === "phone"
                  ? "验证码将发送到您的手机，无需输入密码。"
                  : "验证码将发送到您的邮箱，无需输入密码。"}
              </span>
            </div>
          ) : (
            <div className={styles.field}>
              <label className={styles.label} htmlFor="password">
                密码
              </label>
              <input
                id="password"
                className={styles.input}
                type="password"
                value={password}
                onChange={event => setPassword(event.target.value)}
                placeholder="请输入密码"
                autoComplete="current-password"
              />
            </div>
          )}

          {error ? <div className={styles.error}>{error}</div> : null}

          <div className={styles.actions}>
            <button type="submit" className={styles.submitButton} disabled={isDisabled}>
              {submitting ? "登录中..." : "登录"}
            </button>
            <div className={styles.switchLink}>
              还没有账号？
              <button
                type="button"
                style={{ background: "none", border: "none", color: "var(--color-primary-strong)", fontWeight: 600, cursor: "pointer" }}
                onClick={() => navigate("/register", { state: { from } })}
              >
                前往注册
              </button>
            </div>
          </div>
        </form>
      </div>
    </div>
  );
};

export default LoginPage;