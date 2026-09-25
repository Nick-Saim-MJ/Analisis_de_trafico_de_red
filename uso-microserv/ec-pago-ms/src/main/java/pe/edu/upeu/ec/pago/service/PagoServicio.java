package pe.edu.upeu.ec.pago.service;

import pe.edu.upeu.ec.pago.entity.Pago;
import pe.edu.upeu.ec.pago.repository.PagoRepositorio;
import java.util.List;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class PagoServicio {

    private final PagoRepositorio pagoRepositorio;

    public List<Pago> listarPagos() {
        return pagoRepositorio.findAll();
    }

    public Optional<Pago> buscarPagoPorId(Long id) {
        return pagoRepositorio.findById(id);
    }
}
