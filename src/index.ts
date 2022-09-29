import '@hyper-hyper-space/node-env';
import { HashedObject, Hashing, Identity, LinkupAddress, Shuffle } from '@hyper-hyper-space/core';
import { MultiMap } from '@hyper-hyper-space/core/dist/util/multimap';
import { IncomingMessage } from 'http';
import WebSocket, { WebSocketServer } from 'ws';
import { v4 as uuidv4 } from 'uuid';

const verifiedIdPrefix = LinkupAddress.verifiedIdPrefix;

const wss = new WebSocketServer({ host: '0.0.0.0', port: 3002 });

const listeners = new MultiMap<string, WebSocket>();

wss.on('connection', (ws: WebSocket, request: IncomingMessage) => {

    const defaultLinkupId = request.url? request.url.substring(1) : '';

    console.log('received connection from ' + request.url);

    const linkupIds = new Set<string>();
    
    const wsControl: {monitor?: NodeJS.Timeout, alive: boolean} = {alive: true};

    const uuid = uuidv4();

    wsControl.monitor = setInterval(() => {
        if (!wsControl.alive && linkupIds.size === 0) {
            ws.terminate();
            console.log('terminating websocket due to inactivity')
        }
    }, 5000);

    ws.on('message', function message(rawData) {

        wsControl.alive = true;

        const data = rawData.toString('utf-8');

        console.log('received message: ');
        console.log(data);

        const message = JSON.parse(data);

        if (message.action === 'pong') {
            console.log('received pong');
        } else if (message.action === 'listen') {
            const linkupId = message.linkupId as string || defaultLinkupId;

            if (linkupId.slice(0, verifiedIdPrefix.length) === verifiedIdPrefix) {

                const identityHash = linkupId.slice(verifiedIdPrefix.length).split('/')[0];

                try {
                    HashedObject.fromLiteralContextWithValidation(message.idContext).then((id: HashedObject) => {
                        if (id instanceof Identity && 
                            Hashing.toHex(id.hash()) === identityHash &&
                            id.verifySignature(uuid, message.signature || '')) {
                                
                                listeners.add(linkupId, ws);
        
                                console.log('registering listener for ' + linkupId + ' after id validation');
                        } else {
                            console.log('NOT registering listener for ' + linkupId + ': id validation failed');
                        }
                    }).catch((reason: any) => {
                        console.log('ERROR trying to register listener for ' + linkupId + ':');
                        console.log(reason);
                    });    
                } catch (reason: any) {
                    console.log('ERROR trying to register listener for ' + linkupId + ':');
                    console.log(reason);
                }

                
            } else {
                linkupIds.add(linkupId);
                listeners.add(linkupId, ws);
    
                console.log('registering listener for ' + linkupId);
            }


            
        } else if (message.action === 'send') {

            const receiver = message.linkupId || defaultLinkupId;
            const limit = message.limit;

            console.log('trying to send message to ' + receiver);

            if (listeners.hasKey(receiver)) {

                const toRemove = new Array<WebSocket>();
                const allWs = Array.from(listeners.get(receiver));

                Shuffle.array(allWs);

                const targetWs = limit === undefined || allWs.length <= limit? allWs : allWs.slice(0, limit);

                for (const wsToTarget of targetWs) {
                    if (wsToTarget.readyState === wsToTarget.OPEN) {
                        wsToTarget.send(data);
                        console.log('found listener for ' + receiver + ', send message');
                    } else {
                        toRemove.push(wsToTarget);
                    }
                }

                for (const wsToRemove of toRemove) {
                    listeners.delete(receiver, wsToRemove);
                }

            }

        } else if (message.action === 'query') {
            const targets = message.linkupIds;
            const queryId = message.queryId;
            const hits    = new Array<string>();

            for (const target of targets) {
                for (const listenWs of listeners.get(target)) {
                    if (listenWs.readyState === listenWs.OPEN) {
                        hits.push(target);
                        break;
                    }
                }
            }

            console.log('replying to query with id ' + queryId + ' with results: ' + hits);
            ws.send(JSON.stringify({'action': 'query-reply', 'queryId': queryId, 'hits': hits}));
        }
    });

    ws.send(JSON.stringify({'action': 'update-challenge', 'challenge': uuid}))

    ws.on('close', () => {
        for (const linkupId of linkupIds) {
            console.log('removing websocket from ' + linkupId);
            listeners.delete(linkupId, ws);
        }

        if (wsControl.monitor !== undefined) {
            clearInterval(wsControl.monitor);
            wsControl.monitor = undefined;
        }
        
    });
});