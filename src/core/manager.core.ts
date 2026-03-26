import {
  Inject,
  Injectable,
  NotFoundException,
  OnModuleInit,
  UnprocessableEntityException,
} from '@nestjs/common';
import {
  AppsService,
  IAppsService,
} from '@waha/apps/app_sdk/services/IAppsService';
import { EngineBootstrap } from '@waha/core/abc/EngineBootstrap';
import { GowsEngineConfigService } from '@waha/core/config/GowsEngineConfigService';
import { WPPEngineConfigService } from '@waha/core/config/WPPEngineConfigService';
import { WebJSEngineConfigService } from '@waha/core/config/WebJSEngineConfigService';
import { WhatsappSessionGoWSCore } from '@waha/core/engines/gows/session.gows.core';
import { WebhookConductor } from '@waha/core/integrations/webhooks/WebhookConductor';
import { MediaStorageFactory } from '@waha/core/media/MediaStorageFactory';
import { DefaultMap } from '@waha/utils/DefaultMap';
import { getPinoLogLevel, LoggerBuilder } from '@waha/utils/logging';
import { promiseTimeout, sleep } from '@waha/utils/promiseTimeout';
import { PinoLogger } from 'nestjs-pino';
import { Observable, Subject, Subscription } from 'rxjs';
import { filter, map } from 'rxjs/operators';

import { getNamespace, getSessionNamespace } from '../config';
import { WhatsappConfigService } from '../config.service';
import {
  WAHAEngine,
  WAHAEvents,
  WAHASessionStatus,
} from '../structures/enums.dto';
import {
  ProxyConfig,
  SessionConfig,
  SessionDetailedInfo,
  SessionDTO,
  SessionInfo,
} from '../structures/sessions.dto';
import { WebhookConfig } from '../structures/webhooks.config.dto';
import { populateSessionInfo, SessionManager } from './abc/manager.abc';
import { SessionParams, WhatsappSession } from './abc/session.abc';
import { EngineConfigService } from './config/EngineConfigService';
import { WhatsappSessionNoWebCore } from './engines/noweb/session.noweb.core';
import { WhatsappSessionWPPCore } from './engines/wpp/session.wpp.core';
import { WhatsappSessionWebJSCore } from './engines/webjs/session.webjs.core';
import { WAHA_MAX_SESSIONS } from './env';
import { getProxyConfig } from './helpers.proxy';
import { MediaManager } from './media/MediaManager';
import { LocalSessionAuthRepository } from './storage/LocalSessionAuthRepository';
import { LocalSessionConfigRepository } from './storage/LocalSessionConfigRepository';
import { LocalStoreCore } from './storage/LocalStoreCore';
import { CoreApiKeyRepository } from './storage/CoreApiKeyRepository';

@Injectable()
export class SessionManagerCore extends SessionManager implements OnModuleInit {
  SESSION_STOP_TIMEOUT = 3000;

  //
  // session name -> WhatsappSession (running) | null (stopped)
  // sessions not in the map do not exist
  //
  private sessions: Map<string, WhatsappSession | null>;
  private readonly maxSessions: number;

  protected readonly EngineClass: typeof WhatsappSession;
  protected events2: DefaultMap<WAHAEvents, Subject<any>>;
  private sessionSubscriptions: Map<string, Subscription[]>;
  protected readonly engineBootstrap: EngineBootstrap;

  constructor(
    config: WhatsappConfigService,
    private engineConfigService: EngineConfigService,
    private webjsEngineConfigService: WebJSEngineConfigService,
    private wppEngineConfigService: WPPEngineConfigService,
    gowsConfigService: GowsEngineConfigService,
    log: PinoLogger,
    private mediaStorageFactory: MediaStorageFactory,
    @Inject(AppsService)
    appsService: IAppsService,
  ) {
    super(log, config, gowsConfigService, appsService);
    this.sessions = new Map();
    this.sessionSubscriptions = new Map();
    this.maxSessions = WAHA_MAX_SESSIONS;

    const engineName = this.engineConfigService.getDefaultEngineName();
    this.EngineClass = this.getEngine(engineName);
    this.engineBootstrap = this.getEngineBootstrap(engineName);

    this.events2 = new DefaultMap<WAHAEvents, Subject<any>>(
      (key) => {
        void key;
        return new Subject<any>();
      },
    );

    this.store = new LocalStoreCore(getNamespace(), getSessionNamespace());
    this.sessionAuthRepository = new LocalSessionAuthRepository(this.store);
    this.sessionConfigRepository = new LocalSessionConfigRepository(this.store);
  }

  protected getEngine(engine: WAHAEngine): typeof WhatsappSession {
    if (engine === WAHAEngine.WEBJS) {
      return WhatsappSessionWebJSCore;
    } else if (engine === WAHAEngine.WPP) {
      return WhatsappSessionWPPCore;
    } else if (engine === WAHAEngine.NOWEB) {
      return WhatsappSessionNoWebCore;
    } else if (engine === WAHAEngine.GOWS) {
      return WhatsappSessionGoWSCore;
    } else {
      throw new NotFoundException(`Unknown whatsapp engine '${engine}'.`);
    }
  }

  async beforeApplicationShutdown(signal?: string) {
    for (const [name, session] of this.sessions) {
      if (session) {
        await this.stop(name, true);
      }
    }
    this.stopEvents();
    await this.engineBootstrap.shutdown();
  }

  async onApplicationBootstrap() {
    this.apiKeyRepository = new CoreApiKeyRepository();
    await this.engineBootstrap.bootstrap();
    this.startPredefinedSessions();
    if (this.config.shouldRestartAllSessions) {
      this.startPersistedSessions();
    }
  }

  private startPersistedSessions() {
    for (const [name, session] of this.sessions) {
      if (session) {
        continue;
      }
      this.withLock(name, async () => {
        const log = this.log.logger.child({ session: name });
        log.info(`Auto-starting persisted session...`);
        await this.start(name).catch((error) => {
          log.error(`Failed to auto-start persisted session: ${error}`);
          log.error(error.stack);
        });
      });
    }
  }

  private getRunningCount(): number {
    let count = 0;
    for (const session of this.sessions.values()) {
      if (session) {
        count++;
      }
    }
    return count;
  }

  //
  // API Methods
  //
  async exists(name: string): Promise<boolean> {
    return this.sessions.has(name);
  }

  isRunning(name: string): boolean {
    return !!this.sessions.get(name);
  }

  async upsert(name: string, config?: SessionConfig): Promise<void> {
    await this.sessionConfigRepository.saveConfig(name, config ?? {});
    if (!this.sessions.has(name)) {
      this.sessions.set(name, null);
    }
  }

  async start(name: string): Promise<SessionDTO> {
    if (!this.sessions.has(name)) {
      await this.upsert(name, null);
    }

    if (this.sessions.get(name)) {
      throw new UnprocessableEntityException(
        `Session '${name}' is already started.`,
      );
    }

    if (this.getRunningCount() >= this.maxSessions) {
      throw new UnprocessableEntityException(
        `Max sessions limit (${this.maxSessions}) reached. Stop or delete another session first.`,
      );
    }

    this.log.info({ session: name }, `Starting session...`);
    const logger = this.log.logger.child({ session: name });
    const sessionConfig = await this.sessionConfigRepository.getConfig(name);
    logger.level = getPinoLogLevel(sessionConfig?.debug);
    const loggerBuilder: LoggerBuilder = logger;

    const storage = await this.mediaStorageFactory.build(
      name,
      loggerBuilder.child({ name: 'Storage' }),
    );
    await storage.init();
    const mediaManager = new MediaManager(
      storage,
      this.config.mimetypes,
      loggerBuilder.child({ name: 'MediaManager' }),
    );

    const webhook = new WebhookConductor(loggerBuilder);
    const proxyConfig = this.getProxyConfig(name, sessionConfig);
    const sessionParams: SessionParams = {
      name,
      mediaManager,
      loggerBuilder,
      printQR: this.engineConfigService.shouldPrintQR,
      sessionStore: this.store,
      proxyConfig: proxyConfig,
      sessionConfig: sessionConfig,
      ignore: this.ignoreChatsConfig(sessionConfig),
    };
    if (this.EngineClass === WhatsappSessionWebJSCore) {
      sessionParams.engineConfig = this.webjsEngineConfigService.getConfig();
    } else if (this.EngineClass === WhatsappSessionWPPCore) {
      sessionParams.engineConfig = this.wppEngineConfigService.getConfig();
    } else if (this.EngineClass === WhatsappSessionGoWSCore) {
      sessionParams.engineConfig = this.gowsConfigService.getConfig();
    }
    await this.sessionAuthRepository.init(name);
    // @ts-ignore
    const session = new this.EngineClass(sessionParams);
    this.sessions.set(name, session);
    this.subscribeSession(session);

    // configure webhooks
    const webhooks = this.getWebhooks(sessionConfig);
    webhook.configure(session, webhooks);

    // Apps
    try {
      await this.appsService.beforeSessionStart(session, this.store);
    } catch (e) {
      logger.error(`Apps Error: ${e}`);
      session.status = WAHASessionStatus.FAILED;
    }

    // start session
    if (session.status !== WAHASessionStatus.FAILED) {
      await session.start();
      logger.info('Session has been started.');
      // Apps
      await this.appsService.afterSessionStart(session, this.store);
    }

    return {
      name: session.name,
      status: session.status,
      config: session.sessionConfig,
    };
  }

  private subscribeSession(session: WhatsappSession) {
    const subs: Subscription[] = [];
    for (const eventName in WAHAEvents) {
      const event = WAHAEvents[eventName];
      const stream$ = session
        .getEventObservable(event)
        .pipe(map(populateSessionInfo(event, session)));
      const sub = stream$.subscribe((data) => {
        this.events2.get(event).next(data);
      });
      subs.push(sub);
    }
    this.sessionSubscriptions.set(session.name, subs);
  }

  private unsubscribeSession(sessionName: string) {
    const subs = this.sessionSubscriptions.get(sessionName) ?? [];
    for (const sub of subs) {
      sub.unsubscribe();
    }
    this.sessionSubscriptions.delete(sessionName);
  }

  getSessionEvent(sessionName: string, event: WAHAEvents): Observable<any> {
    const stream$ = this.events2.get(event).asObservable();
    if (sessionName === '*') {
      return stream$;
    }
    return stream$.pipe(filter((data) => data.session === sessionName));
  }

  async stop(name: string, silent: boolean): Promise<void> {
    if (!this.isRunning(name)) {
      this.log.debug({ session: name }, `Session is not running.`);
      return;
    }

    this.log.info({ session: name }, `Stopping session...`);
    try {
      const session = this.getSession(name);
      await session.stop();
    } catch (err) {
      this.log.warn(`Error while stopping session '${name}'`);
      if (!silent) {
        throw err;
      }
    }
    this.log.info({ session: name }, `Session has been stopped.`);
    this.sessions.set(name, null);
    this.unsubscribeSession(name);
    await sleep(this.SESSION_STOP_TIMEOUT);
  }

  async unpair(name: string) {
    const session = this.sessions.get(name) as WhatsappSession | null;
    if (!session) {
      return;
    }

    this.log.info({ session: name }, 'Unpairing the device from account...');
    await session.unpair().catch((err) => {
      this.log.warn(`Error while unpairing from device: ${err}`);
    });
    await sleep(1000);
  }

  async logout(name: string): Promise<void> {
    await this.sessionAuthRepository.clean(name);
  }

  async delete(name: string): Promise<void> {
    await this.appsService.removeBySession(this, name);
    this.unsubscribeSession(name);
    this.sessions.delete(name);
    await this.sessionConfigRepository.deleteConfig(name);
  }

  /**
   * Combine per-session and global webhooks
   */
  private getWebhooks(sessionConfig?: SessionConfig | null) {
    let webhooks: WebhookConfig[] = [];
    if (sessionConfig?.webhooks) {
      webhooks = webhooks.concat(sessionConfig.webhooks);
    }
    const globalWebhookConfig = this.config.getWebhookConfig();
    if (globalWebhookConfig) {
      webhooks.push(globalWebhookConfig);
    }
    return webhooks;
  }

  /**
   * Get either session's or global proxy if defined
   */
  protected getProxyConfig(
    name: string,
    sessionConfig?: SessionConfig | null,
  ): ProxyConfig | undefined {
    if (sessionConfig?.proxy) {
      return sessionConfig.proxy;
    }
    const sessions: Record<string, WhatsappSession> = {};
    for (const [n, s] of this.sessions) {
      if (s) {
        sessions[n] = s;
      }
    }
    return getProxyConfig(this.config, sessions, name);
  }

  getSession(name: string): WhatsappSession {
    const session = this.sessions.get(name);
    if (!session) {
      throw new NotFoundException(
        `We didn't find a session with name '${name}'.\n` +
          `Please start it first by using POST /api/sessions/${name}/start request`,
      );
    }
    return session as WhatsappSession;
  }

  async getSessions(all: boolean): Promise<SessionInfo[]> {
    const result: SessionInfo[] = [];

    if (all) {
      for (const [name, session] of this.sessions) {
        if (session) {
          const me = session.getSessionMeInfo();
          result.push({
            name: session.name,
            status: session.status,
            config: session.sessionConfig,
            me: me,
            presence: session.presence,
            timestamps: {
              activity: session.getLastActivityTimestamp(),
            },
          });
        } else {
          const config = await this.sessionConfigRepository.getConfig(name);
          result.push({
            name: name,
            status: WAHASessionStatus.STOPPED,
            config: config,
            me: null,
            presence: null,
            timestamps: {
              activity: null,
            },
          });
        }
      }
    } else {
      for (const [name, session] of this.sessions) {
        void name;
        if (session) {
          const me = session.getSessionMeInfo();
          result.push({
            name: session.name,
            status: session.status,
            config: session.sessionConfig,
            me: me,
            presence: session.presence,
            timestamps: {
              activity: session.getLastActivityTimestamp(),
            },
          });
        }
      }
    }

    return result;
  }

  private async fetchEngineInfo(name: string) {
    const session = this.sessions.get(name) as WhatsappSession | null;
    let engineInfo = {};
    if (session) {
      try {
        engineInfo = await promiseTimeout(1000, session.getEngineInfo());
      } catch (error) {
        this.log.debug(
          { session: name, error: `${error}` },
          'Can not get engine info',
        );
      }
    }
    return {
      engine: session?.engine,
      ...engineInfo,
    };
  }

  async getSessionInfo(name: string): Promise<SessionDetailedInfo | null> {
    const exists = await this.exists(name);
    if (!exists) {
      return null;
    }
    const sessions = await this.getSessions(true);
    const session = sessions.find((s) => s.name === name);
    if (!session) {
      return null;
    }
    const engine = await this.fetchEngineInfo(name);
    return {
      ...session,
      engine: engine,
    };
  }

  protected stopEvents() {
    for (const subject of this.events2.values()) {
      subject.complete();
    }
  }

  async onModuleInit() {
    await this.init();
  }

  async init() {
    await this.store.init();
    const knex = this.store.getWAHADatabase();
    await this.appsService.migrate(knex);
    // Load all persisted sessions into memory as stopped
    const sessionNames = await this.sessionConfigRepository.getAllConfigs();
    for (const name of sessionNames) {
      if (!this.sessions.has(name)) {
        this.sessions.set(name, null);
      }
    }
  }
}

