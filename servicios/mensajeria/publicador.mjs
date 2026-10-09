// L7 · Lo unico de prestamos.mjs que sabe de amqplib. La URL sale de servicios/.env.
import { connect } from 'amqplib';
import { randomUUID } from 'node:crypto';
import { EXCHANGES, declararTopologia } from './topologia.mjs';

let canal;

export async function conectarPublicador(url) {
  const conexion = await connect(url);
  canal = await conexion.createChannel();
  await declararTopologia(canal);
}

export function publicar(routingKey, payload) {
  const eventoId = randomUUID();
  const aceptado = canal.publish(EXCHANGES.eventos.nombre, routingKey, Buffer.from(JSON.stringify(payload)), {
    persistent: true,
    contentType: 'application/json',
    headers: { 'x-evento-id': eventoId, 'x-emitido-en': new Date().toISOString() },
  });
  console.log(`[prestamos] publicado ${routingKey} evento ${eventoId} aceptado=${aceptado}`);
}